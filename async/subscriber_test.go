//go:build integration

package async

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/encoding"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/streadway/amqp"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	host := "amqp://guest:guest@localhost:5672/"
	exchange := "krakend-testing"

	cfg := Subscriber{
		Topic: "#",
		Endpoint: &config.EndpointConfig{
			Backend: []*config.Backend{
				{
					URLPattern: "/__debug/",
					Host:       []string{"http://localhost:8000"},
					Decoder:    encoding.JSONDecoder,
				},
			},
		},
		Timeout: time.Second,
		ExtraConfig: config.ExtraConfig{
			consumerNamespace: map[string]interface{}{
				"name":           "krakend-test",
				"host":           host,
				"exchange":       exchange,
				"delete":         true,
				"no_wait":        true,
				"prefetch_count": 1,
			},
		},
	}

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")
	defer func() {
		cancel()
		fmt.Println(buf.String())
	}()

	p, err := proxy.DefaultFactory(l).New(cfg.Endpoint)
	if err != nil {
		fmt.Println(err)
		return
	}

	pingChan := make(chan<- string)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timeout := 5 * time.Second

	defer func() {
		<-time.After(timeout)
		cancel()
	}()

	opts := Options{
		Logger:     l,
		Proxy:      p,
		Ping:       pingChan,
		PingTicker: ticker,
	}
	if err := New(ctx, cfg, opts); err != nil {
		fmt.Println(err)
		return
	}

	go func() { fmt.Println(publish(host, exchange, 1)) }()

	<-time.After(timeout + time.Second)

	// output:
	// foo
}

func ExamplePublish() {
	host := "amqp://guest:guest@localhost:5672/"
	exchange := "foo"
	fmt.Println(publish(host, exchange, 2000000))

	// output:
	// <nil>
}

func publish(host, exchange string, iterations int) error {
	conn, err := amqp.Dial(host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	/*
		if err := ch.ExchangeDeclare(
			exchange, // name
			"topic",  // type
			false,    // durable
			false,    // delete when unused
			false,    // exclusive
			false,    // no-wait
			nil,      // arguments
		); err != nil {
			return err
		}
	*/
	body := `{"msg":"Hello World!"}`
	for i := 0; i < iterations; i++ {
		err = ch.Publish(
			exchange,  // exchange
			"foo.bar", // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(body),
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestRateLimited(t *testing.T) {
	t.Errorf("can this fail, please?")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	host := "amqp://guest:guest@localhost:5672/"
	exchange := "krakend-testing"

	cfg := Subscriber{
		Topic: "#",
		Endpoint: &config.EndpointConfig{
			Backend: []*config.Backend{
				{
					URLPattern: "/__debug/",
					Host:       []string{"http://localhost:8000"},
					Decoder:    encoding.JSONDecoder,
				},
			},
		},
		Timeout: time.Second,
		ExtraConfig: config.ExtraConfig{
			consumerNamespace: map[string]interface{}{
				"name":           "krakend-test",
				"host":           host,
				"exchange":       exchange,
				"delete":         true,
				"no_wait":        true,
				"prefetch_count": 1,
			},
		},
	}

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")
	defer func() {
		cancel()
		fmt.Println(buf.String())
	}()

	p, err := proxy.DefaultFactory(l).New(cfg.Endpoint)
	if err != nil {
		fmt.Println(err)
		return
	}

	pingChan := make(chan<- string)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timeout := 5 * time.Second

	defer func() {
		<-time.After(timeout)
		cancel()
	}()

	opts := Options{
		Logger:     l,
		Proxy:      p,
		Ping:       pingChan,
		PingTicker: ticker,
	}
	if err := New(ctx, cfg, opts); err != nil {
		fmt.Println(err)
		return
	}

	go func() { fmt.Println(publish(host, exchange, 1)) }()

	<-time.After(timeout + time.Second)
}
