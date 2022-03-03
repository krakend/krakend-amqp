package async

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/luraproject/lura/v2/async"
	"github.com/luraproject/lura/v2/logging"
)

const minExecutionTime = 5 * time.Second

func StartAgent(ctx context.Context, opts async.Options) bool {
	amqpF := func(ctx context.Context, l logging.Logger) error {
		return New(
			ctx,
			Subscriber{
				Name:        opts.Agent.Name,
				Topic:       opts.Agent.Consumer.Topic,
				Endpoint:    opts.Endpoint,
				Timeout:     opts.Agent.Consumer.Timeout,
				ExtraConfig: opts.Agent.ExtraConfig,
				Workers:     opts.Agent.Consumer.Workers,
				MaxRate:     opts.Agent.Consumer.MaxRate,
			},
			Options{
				Logger:     l,
				Proxy:      opts.Proxy,
				Ping:       opts.AgentPing,
				PingTicker: time.NewTicker(opts.Agent.Connection.HealthInterval),
			},
		)
	}
	shortCtx, localCancel := context.WithTimeout(ctx, time.Second)
	defer localCancel()

	if err := amqpF(shortCtx, logging.NoOp); err == ErrNoConsumerCfgDefined {
		return false
	}

	opts.G.Go(func() error {
		for i := 0; opts.ShouldContinue(i); i++ {
			delay := opts.BackoffF(i)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			start := time.Now()
			if err := amqpF(ctx, opts.Logger); err != nil {
				opts.Logger.Error(fmt.Sprintf("[SERVICE: Asyncagent][%s] building the amqp subscriber:", opts.Agent.Name), err)
			}
			if time.Since(start) > minExecutionTime {
				// if the agent ran for longer than the minimal required, restart the counter
				i = 0
			}
		}
		return ErrTooManyRetries
	})

	return true
}

var ErrTooManyRetries = errors.New("too many retries")
