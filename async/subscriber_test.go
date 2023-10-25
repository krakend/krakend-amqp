//go:build integration

package async

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/encoding"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/streadway/amqp"
)

/*
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

	pingChan := make(chan<- string, 10)
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
*/

/*
func ExamplePublish() {
	host := "amqp://guest:guest@localhost:5672/"
	exchange := "foo"
	fmt.Println(publish(host, exchange, 2000000))

	// output:
	// <nil>
}
*/

func publish(host, exchange string, iterations int, t *testing.T) error {
	conn, err := amqp.Dial(host)
	if err != nil {
		return fmt.Errorf("cannot publish to %s, err: %s", host, err.Error())
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("cannot get channel, err: %s", err.Error())
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
			return fmt.Errorf("cannot publish on exchange: %s, err: %s", exchange, err.Error())
		} else {
			if t != nil {
				t.Logf("message published: %s  (exchange: %s, topic: %s)",
					body, exchange, "foo.bar")
			}
		}
	}
	return nil
}

type fakeHandler struct {
	Received chan int
	t        *testing.T
}

func (h *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	if len(b) > 0 && h.t != nil {
		h.t.Logf("received body: %s", b)
	}
	w.Write(b)
	h.Received <- 1
}

func newFakeHandler(t *testing.T) *fakeHandler {
	return &fakeHandler{
		Received: make(chan int, 100),
		t:        t,
	}
}

func TestRateLimited(t *testing.T) {

	h := newFakeHandler(t)
	s := httptest.NewServer(h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// limit the running time of this test:
	timeout := 5 * time.Second
	defer func() {
		<-time.After(timeout)
		t.Errorf("timing out test after %#v", timeout)
		cancel()
	}()

	host := "amqp://guest:guest@localhost:5672/"
	exchange := "krakend-testing"

	cfg := Subscriber{
		Topic: "#",
		Endpoint: &config.EndpointConfig{
			Backend: []*config.Backend{
				{
					URLPattern: "/__debug/",
					Host:       []string{s.URL},
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
		Workers: 1,
	}

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")
	defer func() {
		cancel()
		t.Logf("buf contains: [[ %s ]]", buf.String())
	}()

	p, err := proxy.DefaultFactory(l).New(cfg.Endpoint)
	if err != nil {
		t.Errorf("cannot create default factory: %s", err.Error())
		return
	}

	pingChan := make(chan string)
	// keep consuming pings from the worker, otherwise the initial ppin
	// will block in the subscriber
	go func() {
		for {
			select {
			case strPing := <-pingChan:
				t.Logf("received ping [%s]", strPing)
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	opts := Options{
		Logger:     l,
		Proxy:      p,
		Ping:       pingChan,
		PingTicker: ticker,
	}

	// the agent must be launch in a goroutine, because the New subscriber
	// blocks in a loop.
	go func() {
		t.Logf("New cfg: %#v", cfg)
		t.Logf("New opts: %#v", opts)
		if err := New(ctx, cfg, opts); err != nil {
			t.Errorf("async agent exited with an error: %s", err.Error())
			return
		}
	}()

	go func() {
		for i := 0; i < 3; i++ {
			if err := publish(host, exchange, 1, t); err != nil {
				t.Errorf("cannot publish in goroutine: %s", err.Error())
			} else {
				t.Logf("pu pu pu")
			}
		}
	}()

	select {
	case <-time.After(timeout + time.Second):
		t.Errorf("timed out at the end")
	case <-h.Received:
		t.Logf("test passed")
	}
}
