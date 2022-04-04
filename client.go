package amqp

import (
	"context"
	"sync"

	"github.com/streadway/amqp"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) proxy.BackendFactory {
	f := backendFactory{
		logger:    logger,
		bf:        bf,
		ctx:       ctx,
		mu:        new(sync.Mutex),
		consumers: map[string]<-chan amqp.Delivery{},
	}

	return f.New
}

type backendFactory struct {
	ctx       context.Context
	logger    logging.Logger
	bf        proxy.BackendFactory
	consumers map[string]<-chan amqp.Delivery
	mu        *sync.Mutex
}

func (f backendFactory) New(remote *config.Backend) proxy.Proxy {
	if prxy, err := f.initConsumer(f.ctx, remote); err == nil {
		return prxy
	}

	if prxy, err := f.initProducer(f.ctx, remote); err == nil {
		return prxy
	}

	return f.bf(remote)
}

func (backendFactory) newChannel(path string) (*amqp.Channel, closer, error) {
	conn, err := amqp.Dial(path)
	if err != nil {
		return nil, nopCloser, err
	}
	ch, err := conn.Channel()
	return ch, conn.Close, nil
}

type closer func() error

func nopCloser() error { return nil }

type queueCfg struct {
	Name          string   `json:"name"`
	Exchange      string   `json:"exchange"`
	RoutingKey    []string `json:"routing_key"`
	Durable       bool     `json:"durable"`
	Delete        bool     `json:"delete"`
	Exclusive     bool     `json:"exclusive"`
	NoWait        bool     `json:"no_wait"`
	PrefetchCount int      `json:"prefetch_count"`
	PrefetchSize  int      `json:"prefetch_size"`
}
