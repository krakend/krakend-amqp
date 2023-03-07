package amqp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/streadway/amqp"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/proxy"
)

const consumerNamespace = "github.com/devopsfaith/krakend-amqp/consume"

var (
	errNoConsumerCfgDefined = errors.New("no amqp consumer defined")
	errNoBackendHostDefined = errors.New("no host backend defined")
)

type consumerCfg struct {
	queueCfg
	AutoACK bool `json:"auto_ack"`
	NoLocal bool `json:"no_local"`
}

func (f backendFactory) initConsumer(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}
	dns := remote.Host[0]
	logPrefix := "[BACKEND: " + remote.URLPattern + "][AMQP]"
	cfg, err := getConsumerConfig(remote)
	if err != nil {
		if err != errNoConsumerCfgDefined {
			f.logger.Debug(logPrefix, fmt.Sprintf("%s: %s", dns, err.Error()))
		}
		return proxy.NoopProxy, err
	}
	cfg.LogPrefix = logPrefix

	connHandler := newConnectionHandler(ctx, f.logger, cfg.LogPrefix)
	msgs, err := connHandler.newConsumer(dns, cfg, DefaultStartupRetries, DefaultBackoffStrategy)
	if err != nil {
		f.logger.Debug(logPrefix, err.Error())
	}

	f.logger.Debug(logPrefix, "Consumer attached")
	go func() {
		<-ctx.Done()
		connHandler.conn.Close()
	}()

	ef := proxy.NewEntityFormatter(remote)
	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				// executed here instead of in the connection manager `connect` method
				// to avoid launching too many goroutines that will do nothing because
				// the atomic.Bool will be locked
				if connHandler.reconnecting.CompareAndSwap(false, true) {
					go func() {
						msgs, err = connHandler.newConsumer(dns, cfg, cfg.MaxRetries, cfg.Backoff)
						if err != nil {
							f.logger.Debug(logPrefix, err.Error())
						}
					}()
				}
				return nil, fmt.Errorf("connection not available, trying to reconnect")
			}
			var data map[string]interface{}
			err := remote.Decoder(bytes.NewBuffer(msg.Body), &data)
			if err != nil && err != io.EOF {
				msg.Nack(false, true)
				return nil, err
			}

			msg.Ack(false)

			newResponse := proxy.Response{Data: data, IsComplete: true}
			newResponse = ef.Format(newResponse)
			return &newResponse, nil
		}
	}, nil
}

func getConsumerConfig(remote *config.Backend) (*consumerCfg, error) {
	v, ok := remote.ExtraConfig[consumerNamespace]
	if !ok {
		return nil, errNoConsumerCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &consumerCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

// newConsumer needs to execute connect first because it blocks the execution
func (h *connectionHandler) newConsumer(dns string, cfg *consumerCfg, maxRetries int, bckoff string) (<-chan amqp.Delivery, error) {
	emptyChan := make(chan amqp.Delivery)
	close(emptyChan)

	if err := h.connect(dns, maxRetries, bckoff); err != nil {
		return emptyChan, fmt.Errorf("getting the channel for %s/%s: %s", dns, cfg.Name, err.Error())
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
		return emptyChan, fmt.Errorf("declaring the exchange for %s/%s: %s", dns, cfg.Name, err.Error())
	}

	q, err := h.conn.ch.QueueDeclare(
		cfg.Name,
		cfg.Durable,
		cfg.Delete,
		cfg.Exclusive,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		h.conn.Close()
		return emptyChan, fmt.Errorf("declaring the queue for %s/%s: %s", dns, cfg.Name, err.Error())
	}

	for _, k := range cfg.RoutingKey {
		err := h.conn.ch.QueueBind(
			q.Name,       // queue name
			k,            // routing key
			cfg.Exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			h.logger.Error(cfg.LogPrefix, fmt.Sprintf("bindind the queue for %s/%s: %s", dns, cfg.Name, err.Error()))
		}
	}

	if cfg.PrefetchCount != 0 || cfg.PrefetchSize != 0 {
		if err := h.conn.ch.Qos(cfg.PrefetchCount, cfg.PrefetchSize, false); err != nil {
			h.conn.Close()
			return emptyChan, fmt.Errorf("setting the QoS for the consumer %s/%s: %s", dns, cfg.Name, err.Error())
		}
	}

	msgs, err := h.conn.ch.Consume(
		cfg.Name,
		"", // cfg.Exchange,
		cfg.AutoACK,
		cfg.Exclusive,
		cfg.NoLocal,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		h.conn.Close()
		return emptyChan, fmt.Errorf("setting up the consumer for %s/%s: %s", dns, cfg.Name, err.Error())
	}
	return msgs, nil
}
