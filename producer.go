package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	"github.com/luraproject/lura/config"
	"github.com/luraproject/lura/proxy"
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

	cfg, err := getProducerConfig(remote)
	if err != nil {
		f.logger.Debug(logPrefix, fmt.Sprintf("%s: %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	ch, close, err := f.newChannel(dns)
	if err != nil {
		f.logger.Error(logPrefix, fmt.Sprintf("Error getting the channel for %s/%s: %s", dns, cfg.Name, err.Error()))
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
		f.logger.Error(logPrefix, fmt.Sprintf("Error declaring the exchange for %s/%s: %s", dns, cfg.Name, err.Error()))
		close()
		return proxy.NoopProxy, err
	}

	go func() {
		<-ctx.Done()
		close()
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
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

		err = ch.Publish(
			cfg.Exchange,
			r.Params[cfg.RoutingKey],
			cfg.Mandatory,
			cfg.Immediate,
			pub,
		)
		if err != nil {
			return nil, err
		}
		return &proxy.Response{IsComplete: true}, nil
	}, nil
}
