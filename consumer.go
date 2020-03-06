package amqp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/streadway/amqp"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/proxy"
)

const consumerNamespace = "github.com/devopsfaith/krakend-amqp/consume"

var errNoConsumerCfgDefined = errors.New("no amqp consumer defined")
var errNoBackendHostDefined = errors.New("no host backend defined")

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

	cfg, err := getConsumerConfig(remote)
	if err != nil {
		f.logger.Debug(fmt.Sprintf("AMQP: %s: %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if msgs, ok := f.consumers[dns+cfg.Name]; ok {
		return consumerBackend(remote, msgs), nil
	}

	ch, close, err := f.newChannel(dns)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: getting the channel for %s/%s: %s", dns, cfg.Name, err.Error()))
		return proxy.NoopProxy, err
	}

	err = ch.ExchangeDeclare(
		cfg.Exchange, // name
		"topic",      // type
		cfg.Durable,
		cfg.Delete,
		cfg.Exclusive,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: declaring the exchange for %s/%s: %s", dns, cfg.Name, err.Error()))
		close()
		return proxy.NoopProxy, err
	}

	q, err := ch.QueueDeclare(
		cfg.Name,
		cfg.Durable,
		cfg.Delete,
		cfg.Exclusive,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: declaring the queue for %s/%s: %s", dns, cfg.Name, err.Error()))
		close()
		return proxy.NoopProxy, err
	}

	for _, k := range cfg.RoutingKey {
		err := ch.QueueBind(
			q.Name,       // queue name
			k,            // routing key
			cfg.Exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			f.logger.Error(fmt.Sprintf("AMQP: bindind the queue for %s/%s: %s", dns, cfg.Name, err.Error()))
		}
	}

	if cfg.PrefetchCount != 0 || cfg.PrefetchSize != 0 {
		if err := ch.Qos(cfg.PrefetchCount, cfg.PrefetchSize, false); err != nil {
			f.logger.Error(fmt.Sprintf("AMQP: setting the QoS for the consumer %s/%s: %s", dns, cfg.Name, err.Error()))
			close()
			return proxy.NoopProxy, err
		}
	}

	msgs, err := ch.Consume(
		cfg.Name,
		"", // cfg.Exchange,
		cfg.AutoACK,
		cfg.Exclusive,
		cfg.NoLocal,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: setting up the consumer for %s/%s: %s", dns, cfg.Name, err.Error()))
		close()
		return proxy.NoopProxy, err
	}

	f.consumers[dns+cfg.Name] = msgs

	go func() {
		<-ctx.Done()
		close()
	}()

	return consumerBackend(remote, msgs), nil
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

func consumerBackend(remote *config.Backend, msgs <-chan amqp.Delivery) proxy.Proxy {
	ef := proxy.NewEntityFormatter(remote)
	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-msgs:
			var data map[string]interface{}
			err := remote.Decoder(bytes.NewBuffer(msg.Body), &data)
			if err != nil && err != io.EOF {
				msg.Ack(false)
				return nil, err
			}

			msg.Ack(true)

			newResponse := proxy.Response{Data: data, IsComplete: true}
			newResponse = ef.Format(newResponse)
			return &newResponse, nil
		}
	}
}
