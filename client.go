package amqp

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"

	"github.com/luraproject/lura/v2/backoff"
	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

var (
	DefaultBackoffStrategy = ""
	DefaultStartupRetries  = 3
)

// NewBackendFactory returns a proxy.BackendFactory that setup AMQP backends
func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) proxy.BackendFactory {
	f := backendFactory{
		logger: logger,
		bf:     bf,
		ctx:    ctx,
	}

	return f.New
}

type backendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
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

// connectionHandler handles the connection to the amqp backend
// the connection is shared by all the requests to that backend
type connectionHandler struct {
	logger       logging.Logger
	logPrefix    string
	reconnecting *atomic.Bool
	conn         connection
}

// newConnectionHandler returns a configured connectionHandler
func newConnectionHandler(ctx context.Context, l logging.Logger, logPrefix string) connectionHandler {
	c := connectionHandler{
		logger:       l,
		logPrefix:    logPrefix,
		reconnecting: new(atomic.Bool),
		conn:         connection{},
	}
	go func() {
		<-ctx.Done()
		c.conn.Close()
	}()
	return c
}

// newConnection should only be used via connect() beacause it doesn't Lock
// to replace the connection
func (h *connectionHandler) newConnection(path string) error {
	c := connection{}
	conn, err := amqp.Dial(path)
	if err != nil {
		return err
	}
	c.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	c.ch = ch
	h.conn = c
	return nil
}

// connect tries to connect to the service with retries given the configuration
// strategy
// WARNING: this function should not be called before setting the reconnecting flag to true
// using the `CompareAndSwap(false, true)`. as a lock method.
func (h *connectionHandler) connect(dns string, maxRetries int, bckoff string) error {
	// This block is executed in the handlers to avoid launching too many goroutines
	// that will do nothing because of the atomic.Bool set to true
	// if !h.reconnecting.CompareAndSwap(false, true) {
	// 	return fmt.Errorf("already reconnecting")
	// }
	var res error
	defer func() {
		h.reconnecting.Store(false)
	}()
	bo := backoff.GetByName(bckoff)
	h.logger.Debug(h.logPrefix, "Connecting to host:", dns)
	if maxRetries == 0 {
		maxRetries = 1
	}
	for i := 0; i < maxRetries; i++ {
		<-time.After(bo(i))
		res = h.newConnection(dns)
		if res == nil {
			h.logger.Info(fmt.Sprintf("Connection attempt #%d:  Successfully connected", i+1))
			return nil
		}
		h.logger.Debug(h.logPrefix, fmt.Sprintf("Connection attempt #%d: %s", i+1, res))
	}
	return res
}

type connection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (c connection) Close() error {
	if c.conn != nil {
		defer c.conn.Close()
	}
	if c.ch == nil {
		return nil
	}
	return c.ch.Close()
}

func (c connection) IsClosed() bool {
	if c.conn == nil {
		return true
	}
	return c.conn.IsClosed()
}

type queueCfg struct {
	Name          string   `json:"name"`
	Exchange      string   `json:"exchange"`
	Backoff       string   `json:"backoff_strategy"`
	RoutingKey    []string `json:"routing_key"`
	Durable       bool     `json:"durable"`
	Delete        bool     `json:"delete"`
	Exclusive     bool     `json:"exclusive"`
	NoWait        bool     `json:"no_wait"`
	PrefetchCount int      `json:"prefetch_count"`
	PrefetchSize  int      `json:"prefetch_size"`
	MaxRetries    int      `json:"max_retries"`
	LogPrefix     string
}
