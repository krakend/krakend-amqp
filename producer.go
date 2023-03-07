package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/proxy"
)

const producerNamespace = "github.com/devopsfaith/krakend-amqp/produce"

var errNoProducerCfgDefined = errors.New("no amqp producer defined")

func getProducerConfig(remote *config.Backend) (*producerCfg, error) {
	v, ok := remote.ExtraConfig[producerNamespace]
	if !ok {
		return nil, errNoProducerCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &producerCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

type producerCfg struct {
	queueCfg
	Mandatory     bool   `json:"mandatory"`
	Immediate     bool   `json:"immediate"`
	ExpirationKey string `json:"exp_key"`
	ReplyToKey    string `json:"reply_to_key"`
	MessageIdKey  string `json:"msg_id_key"`
	PriorityKey   string `json:"priority_key"`
	RoutingKey    string `json:"routing_key"`
}

func (f backendFactory) initProducer(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}
	dns := remote.Host[0]
	logPrefix := "[BACKEND: " + remote.URLPattern + "][AMQP]"

	cfg, err := getProducerConfig(remote)
	if err != nil {
		if err != errNoProducerCfgDefined {
			f.logger.Debug(logPrefix, fmt.Sprintf("%s: %s", dns, err.Error()))
		}
		return proxy.NoopProxy, err
	}
	cfg.LogPrefix = logPrefix

	connHandler := newConnectionHandler(ctx, f.logger, cfg.LogPrefix)
	if err := connHandler.newProducer(dns, cfg, DefaultStartupRetries, DefaultBackoffStrategy); err != nil {
		f.logger.Error(logPrefix, err.Error())
		connHandler.conn.Close()
	}

	f.logger.Debug(logPrefix, "Producer attached")
	go func() {
		<-ctx.Done()
		connHandler.conn.Close()
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
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
			Headers:     headers,
			ContentType: contentType,
			Body:        body,
			Timestamp:   time.Now(),
			Expiration:  r.Params[cfg.ExpirationKey],
			ReplyTo:     r.Params[cfg.ReplyToKey],
			MessageId:   r.Params[cfg.MessageIdKey],
		}

		if len(r.Headers["Content-Type"]) > 0 {
			pub.ContentType = r.Headers["Content-Type"][0]
		}

		if v, ok := r.Params[cfg.PriorityKey]; ok {
			if i, err := strconv.Atoi(v); err == nil {
				pub.Priority = uint8(i)
			}
		}

		if connHandler.conn.IsClosed() {
			if connHandler.reconnecting.CompareAndSwap(false, true) {
				go func() {
					if err := connHandler.newProducer(dns, cfg, cfg.MaxRetries, cfg.Backoff); err != nil {
						f.logger.Debug(logPrefix, err.Error())
					}
				}()
			}
			return nil, fmt.Errorf("connection not available, trying to reconnect")
		}

		if err := connHandler.conn.ch.Publish(
			cfg.Exchange,
			r.Params[cfg.RoutingKey],
			cfg.Mandatory,
			cfg.Immediate,
			pub,
		); err != nil {
			if err != amqp.ErrClosed || !connHandler.reconnecting.CompareAndSwap(false, true) {
				return nil, err
			}
			go func() {
				if err = connHandler.newProducer(dns, cfg, cfg.MaxRetries, cfg.Backoff); err != nil {
					f.logger.Debug(logPrefix, err.Error())
				}
			}()
			return nil, err
		}

		return &proxy.Response{IsComplete: true}, nil
	}, nil
}

// newProducer needs to execute connect first because it blocks the execution
func (h *connectionHandler) newProducer(dns string, cfg *producerCfg, maxRetries int, bckoff string) error {
	if err := h.connect(dns, maxRetries, bckoff); err != nil {
		return fmt.Errorf("getting the channel for %s/%s: %s", dns, cfg.Name, err.Error())
	}

	if err := h.conn.ch.ExchangeDeclare(
		cfg.Exchange, // name
		"topic",      // type
		cfg.Durable,
		cfg.Delete,
		cfg.Exclusive,
		cfg.NoWait,
		nil,
	); err != nil {
		h.conn.Close()
		return fmt.Errorf("declaring the exchange for %s/%s: %s", dns, cfg.Name, err.Error())
	}

	return nil
}
