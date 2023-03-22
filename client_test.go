//go:build integration
// +build integration

package amqp

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/encoding"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

var (
	rabbitmqHost    *string = flag.String("rabbitmq", "localhost", "The host of the rabbitmq server")
	totalIterations *int    = flag.Int("iterations", 10000, "The number of produce and consume iterations")
)

func Test(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSize := 10

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")
	defer func() {
		fmt.Println(buf.String())
	}()

	bf := NewBackendFactory(ctx, l, func(_ *config.Backend) proxy.Proxy {
		t.Error("this backend factory shouldn't be called")
		return proxy.NoopProxy
	})
	amqpHost := fmt.Sprintf("amqp://guest:guest@%s:5672", *rabbitmqHost)
	consumerProxy := bf(&config.Backend{
		Host: []string{amqpHost},
		ExtraConfig: config.ExtraConfig{
			consumerNamespace: map[string]interface{}{
				"name":           "queue-1",
				"exchange":       "some-exchange",
				"durable":        true,
				"delete":         false,
				"exclusive":      false,
				"no_wait":        true,
				"auto_ack":       false,
				"no_local":       false,
				"routing_key":    []string{"#"},
				"prefetch_count": batchSize,
			},
		},
		Decoder: encoding.JSONDecoder,
	})

	producerProxy := bf(&config.Backend{
		Host: []string{amqpHost},
		ExtraConfig: config.ExtraConfig{
			producerNamespace: map[string]interface{}{
				"name":      "queue-1",
				"exchange":  "some-exchange",
				"durable":   true,
				"delete":    false,
				"exclusive": false,
				"no_wait":   true,
				"mandatory": true,
				"immediate": false,
			},
		},
	})

	fmt.Println("proxies created. starting the test")

	for i := 0; i < *totalIterations; i++ {
		resp, err := producerProxy(ctx, &proxy.Request{
			Headers: map[string][]string{
				"Content-Type": {"application/json"},
			},
			Params: map[string]string{"routing_key": "some_value"},
			Body:   io.NopCloser(bytes.NewBufferString(fmt.Sprintf("{\"foo\":\"bar\",\"some\":%d}", i))),
		})
		if err != nil {
			t.Error(err)
			return
		}

		if resp == nil || !resp.IsComplete {
			t.Errorf("unexpected response %v", resp)
			return
		}
	}

	for i := 0; i < *totalIterations; i++ {
		localCtx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err := consumerProxy(localCtx, nil)
		cancel()

		if err != nil {
			t.Errorf("#%d: unexpected error %s", i, err.Error())
			return
		}

		if resp == nil || !resp.IsComplete {
			t.Errorf("#%d: unexpected response %v", i, resp)
			return
		}

		res, ok := resp.Data["foo"]
		if !ok {
			t.Errorf("#%d: unexpected response %v", i, resp)
			return
		}
		if v, ok := res.(string); !ok || v != "bar" {
			t.Errorf("#%d: unexpected response %v", i, resp)
			return
		}
	}
}
