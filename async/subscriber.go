package async

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"

	ratelimit "github.com/kakendio/krakend-ratelimit/v3"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

// Subscriber defines the configuration of a single subscriber/consumer to be initialized
// and maintained by the lura service
type Subscriber struct {
	Name  string
	Topic string
	// pipe definition
	Endpoint *config.EndpointConfig
	// timeout of the pipe defined by this subscriber
	Timeout time.Duration
	// Endpoint Extra configuration for customized behaviour
	ExtraConfig config.ExtraConfig
	Workers     int
	MaxRate     float64
}

const consumerNamespace = "github.com/devopsfaith/krakend-amqp/agent"

type Options struct {
	Logger     logging.Logger
	Proxy      proxy.Proxy
	Ping       chan<- string
	PingTicker *time.Ticker
}

// New instantiates and executes an async agent consuming from a rabbitmq
// service. The caller is responsible for reconnections.
func New(ctx context.Context, cfg Subscriber, opts Options) error {
	v, ok := cfg.ExtraConfig[consumerNamespace]
	if !ok {
		return ErrNoConsumerCfgDefined
	}

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	consumrCfg := consumerCfg{}
	if err := json.Unmarshal(b, &consumrCfg); err != nil {
		return err
	}

	options := consumerOptions{
		consumerCfg: consumrCfg,
		Topic:       cfg.Topic,
		Timeout:     cfg.Timeout,
	}

	opts.Logger.Info(fmt.Sprintf("[SERVICE: AsyncAgent][AMQP][%s] Starting the consumer", cfg.Name))
	msgs, closeF, err := initConsumer(options, opts.Logger)
	if err != nil {
		return err
	}
	defer closeF()

	// initial ping
	opts.Ping <- cfg.Name

	shouldAck := newProcessor(ctx, cfg, opts.Logger, opts.Proxy)
	sem := make(chan struct{}, cfg.Workers)
	var shouldExit atomic.Value
	shouldExit.Store(false)
	defer opts.PingTicker.Stop()

	waitIfRequired := func() {}
	if cfg.MaxRate > 0 {
		capacity := int64(cfg.MaxRate)
		if capacity == 0 {
			capacity = 1
		}
		bucket := ratelimit.NewTokenBucket(cfg.MaxRate, capacity)
		// TODO: check if this is Ok:
		pollingTime := time.Millisecond * 10
		waitIfRequired = func() {
			for !bucket.Allow() {
				time.Sleep(pollingTime)
			}
		}
	}

recvLoop:
	for !shouldExit.Load().(bool) {
		// block until an event happens
		// 1: semaphore acquired
		// 2: context ended
		// 3: ping ticker
		select {
		case sem <- struct{}{}:
			// acquire the semaphore
		case <-ctx.Done():
			// agent should stop
			err = ctx.Err()
			break recvLoop
		case <-opts.PingTicker.C:
			// time to ping the router
			opts.Ping <- cfg.Name
			continue
		}

		go func() {
			// release the semaphore
			defer func() { <-sem }()

			var msg amqp.Delivery
			var more bool

			select {
			case <-ctx.Done(): // agent should stop
				return
			case msg, more = <-msgs: // block until a message is read or the chan is closed
				if !more { // channel is closed
					shouldExit.Store(true)
					return
				}
			}

			waitIfRequired()

			ok := shouldAck(msg.Body)
			if consumrCfg.AutoACK {
				// the driver acks the message itself
				return
			}

			if ok {
				if err := msg.Ack(false); err != nil {
					opts.Logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][AMQP][%s] Ack: %s", cfg.Name, err))
					shouldExit.Store(true)
				}
				return
			}

			if err := msg.Nack(false, true); err != nil {
				opts.Logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][AMQP][%s] Nack: %s", cfg.Name, err))
				shouldExit.Store(true)
			}
		}()
	}

	opts.Logger.Warning(fmt.Sprintf("[SERVICE: AsyncAgent][AMQP][%s] Consumer stopped", cfg.Name))

	return err
}

type consumerCfg struct {
	Host          string `json:"host"`
	Name          string `json:"name"`
	Exchange      string `json:"exchange"`
	Durable       bool   `json:"durable"`
	Delete        bool   `json:"delete"`
	Exclusive     bool   `json:"exclusive"`
	NoWait        bool   `json:"no_wait"`
	PrefetchCount int    `json:"prefetch_count"`
	PrefetchSize  int    `json:"prefetch_size"`
	AutoACK       bool   `json:"auto_ack"`
	NoLocal       bool   `json:"no_local"`
}

type consumerOptions struct {
	consumerCfg
	Topic    string
	Encoding string
	Timeout  time.Duration
}

func newProcessor(ctx context.Context, cfg Subscriber, logger logging.Logger, next proxy.Proxy) func([]byte) bool {
	return func(msg []byte) bool {
		req := proxy.Request{
			Params:  map[string]string{},
			Headers: map[string][]string{},
			Body:    io.NopCloser(bytes.NewBuffer(msg)),
		}
		contxt, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()

		_, err := next(contxt, &req)

		if err == nil {
			return true
		}
		logger.Error(fmt.Sprintf("[SERVICE: AsyncAgent][AMQP][%s] %s", cfg.Name, err.Error()))
		return false
	}
}

func initConsumer(opts consumerOptions, _ logging.Logger) (msgs <-chan amqp.Delivery, closeF func(), err error) {
	closeF = func() {}

	conn, err1 := amqp.Dial(opts.Host)
	if err1 != nil {
		err = err1
		return
	}

	ch, err1 := conn.Channel()
	if err1 != nil {
		conn.Close()
		err = err1
		return
	}

	closeF = func() {
		ch.Close()
		conn.Close()
	}

	err = ch.ExchangeDeclare(
		opts.Exchange,  // name
		"topic",        // type
		opts.Durable,   // durable
		opts.Delete,    // delete when unused
		opts.Exclusive, // exclusive
		opts.NoWait,    // no-wait
		nil,            // arguments
	)
	if err != nil {
		closeF()
		return
	}

	q, err1 := ch.QueueDeclare(
		opts.Name,      // name
		opts.Durable,   // durable
		opts.Delete,    // delete when unused
		opts.Exclusive, // exclusive
		opts.NoWait,    // no-wait
		nil,            // arguments
	)
	if err1 != nil {
		closeF()
		err = err1
		return
	}
	err = ch.QueueBind(
		q.Name,        // queue name
		opts.Topic,    // routing key
		opts.Exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		closeF()
		return
	}

	if opts.PrefetchCount != 0 || opts.PrefetchSize != 0 {
		if err = ch.Qos(opts.PrefetchCount, opts.PrefetchSize, false); err != nil {
			closeF()
			return
		}
	}

	msgs, err = ch.Consume(
		q.Name,         // queue
		"krakend",      // consumer
		opts.AutoACK,   // auto-ack
		opts.Exclusive, // exclusive
		opts.NoLocal,   // no-local
		opts.NoWait,    // no-wait
		nil,            // args
	)
	if err != nil {
		closeF()
		return
	}

	return
}

var ErrNoConsumerCfgDefined = errors.New("no amqp consumer defined")
