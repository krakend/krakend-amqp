package amqp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/streadway/amqp"
)

const rpcNamespace = "github.com/devopsfaith/krakend-amqp/rpc"

var errNoRPCCfgDefined = errors.New("no amqp rpc handler defined")

func getRPCConfig(remote *config.Backend) (*rpcCfg, error) {
	v, ok := remote.ExtraConfig[rpcNamespace]
	if !ok {
		return nil, errNoRPCCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &rpcCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

type requestExchangeCfg struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Internal   bool   `json:"internal"`
	NoWait     bool   `json:"no_wait"`
}

type responseQueueCfg struct {
	Name      string `json:"name"`
	Consumer  string `json:"consumer"`
	AutoAck   bool   `json:"auto_ack"`
	Exclusive bool   `json:"exclusive"`
	NoLocal   bool   `json:"no_local"`
	NoWait    bool   `json:"no_wait"`
}

var amqpReplyToQueueCfg = responseQueueCfg{Name: "amq.rabbitmq.reply-to", Consumer: "", AutoAck: true, Exclusive: false, NoLocal: false, NoWait: false}

type rpcCfg struct {
	RequestCfg  requestExchangeCfg `json:"request_exchange"`
	ResponseCfg responseQueueCfg   `json:"response_queue"`

	LogPrefix  string `json:"log_prefix"`
	MaxRetries int64  `json:"max_retries"`

	Mandatory  bool   `json:"mandatory"`
	Immediate  bool   `json:"immediate"`
	RoutingKey string `json:"routing_key"`

	ExpirationKey string `json:"exp_key"`
	ReplyToKey    string `json:"reply_to_key"`
	MessageIdKey  string `json:"msg_id_key"`
	PriorityKey   string `json:"priority_key"`
}

func preparePublishing(cfg *rpcCfg, r *proxy.Request, corrId string) (*amqp.Publishing, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	contentType := ""
	headers := amqp.Table{}
	for k, vs := range r.Headers {
		headerValues := make([]interface{}, len(vs))
		for k, v := range vs {
			headerValues[k] = v
		}
		headers[k] = headerValues
	}

	pub := amqp.Publishing{
		Headers:       headers,
		ContentType:   contentType,
		Body:          body,
		Timestamp:     time.Now(),
		Expiration:    r.Params[cfg.ExpirationKey],
		ReplyTo:       cfg.ResponseCfg.Name,
		MessageId:     r.Params[cfg.MessageIdKey],
		CorrelationId: corrId,
	}

	if len(r.Headers["Content-Type"]) > 0 {
		pub.ContentType = r.Headers["Content-Type"][0]
	}

	v, ok := r.Params[cfg.PriorityKey]
	if ok {
		i, err := strconv.ParseUint(v, 10, 8)
		if err == nil {
			pub.Priority = uint8(i)
		}
	}
	return &pub, nil
}

func requestHandler(ctx context.Context, handler connectionHandler, cfg *rpcCfg, r *proxy.Request, remote *config.Backend, corrId string, responseChan chan amqp.Delivery) (*proxy.Response, error) {

	pub, err := preparePublishing(cfg, r, corrId)
	if err != nil {
		return nil, err
	}

	if err := handler.conn.ch.Publish(
		cfg.RequestCfg.Name,
		cfg.RoutingKey,
		cfg.Mandatory,
		cfg.Immediate,
		*pub,
	); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-responseChan:
			var data map[string]interface{}
			err := remote.Decoder(bytes.NewBuffer(msg.Body), &data)
			if err != nil && err != io.EOF {
				msg.Nack(false, true)
				return nil, err
			}

			newResponse := proxy.Response{Data: data, IsComplete: true}
			entityFormatter := proxy.NewEntityFormatter(remote)
			newResponse = entityFormatter.Format(newResponse)
			return &newResponse, nil
		}
	}
}

func (f backendFactory) initRPC(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}
	dns := remote.Host[0]
	logPrefix := "[BACKEND: " + remote.URLPattern + "][AMQP]"

	f.logger.Debug(logPrefix, fmt.Sprintf("Starting rpc handler: %s", dns))

	cfg, err := getRPCConfig(remote)
	if err != nil {
		if err != errNoRPCCfgDefined {
			f.logger.Debug(logPrefix, fmt.Sprintf("%s: %s", dns, err.Error()))
		}
		return proxy.NoopProxy, err
	}

	cfg.LogPrefix = logPrefix

	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = math.MaxInt64
	}

	connHandler := newConnectionHandler(ctx, f.logger, cfg.LogPrefix)

	// channel set up for RPC
	msgs, err := connHandler.newRPC(cfg, dns, DefaultStartupRetries, DefaultBackoffStrategy)
	if err != nil {
		f.logger.Error(cfg.LogPrefix, err.Error())
		connHandler.conn.Close()
	}

	go func() {
		<-ctx.Done()
		f.logger.Debug(cfg.LogPrefix, "Done. Closing connection.")
		connHandler.conn.Close()
	}()

	chanMap := sync.Map{}

	go func() {
		for m := range msgs {
			ch, ok := chanMap.LoadAndDelete(m.CorrelationId)
			if ok {
				channel := ch.(chan amqp.Delivery)
				channel <- m
			}
		}
	}()

	return func(ctx context.Context, request *proxy.Request) (*proxy.Response, error) {
		corrId := uuid.NewString()
		responseChan := make(chan amqp.Delivery)
		chanMap.Store(corrId, responseChan)

		return requestHandler(ctx, connHandler, cfg, request, remote, corrId, responseChan)
	}, nil
}

// newRPC needs to execute connect first because it blocks the execution
func (h *connectionHandler) newRPC(cfg *rpcCfg, dns string, maxRetries int, bckoff string) (<-chan amqp.Delivery, error) {
	var emptyChan chan amqp.Delivery

	err := h.connect(dns, maxRetries, bckoff)
	if err != nil {
		return emptyChan, fmt.Errorf("connection failed for %s: %s", dns, err.Error())
	}

	err = h.conn.ch.ExchangeDeclare(
		cfg.RequestCfg.Name,
		cfg.RequestCfg.Kind,
		cfg.RequestCfg.Durable,
		cfg.RequestCfg.AutoDelete,
		cfg.RequestCfg.Internal,
		cfg.RequestCfg.NoWait,
		nil)

	if err != nil {
		return emptyChan, fmt.Errorf("declaring the exchange for %s: %s", cfg.RequestCfg.Name, err.Error())
	}

	if cfg.ResponseCfg.Name == "" {
		cfg.ResponseCfg = amqpReplyToQueueCfg
	}

	msgs, err := h.conn.ch.Consume(cfg.ResponseCfg.Name, cfg.ResponseCfg.Consumer, cfg.ResponseCfg.AutoAck, cfg.ResponseCfg.Exclusive, cfg.ResponseCfg.NoLocal, cfg.ResponseCfg.NoWait, nil)
	if err != nil {
		return emptyChan, fmt.Errorf("failed to start consuming reply queue: %s", cfg.ResponseCfg.Name)
	}

	return msgs, nil
}
