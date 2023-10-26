//go:build integration

package async

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/encoding"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
	"github.com/streadway/amqp"
)

func publish(host, exchange string, iterations int, content string) error {
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

	body := fmt.Sprintf(`{"msg":"%s"}`, content)
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
		}
	}
	return nil
}

type fakeHandler struct {
	Received    chan int
	data        [][]byte
	numReceived int
	mu          sync.RWMutex
}

func (h *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	w.Write(b)

	h.mu.Lock()
	h.numReceived++
	nR := h.numReceived
	h.data = append(h.data, b)
	h.mu.Unlock()

	h.Received <- nR
}

func (h *fakeHandler) Data(atIdx int) []byte {
	var b []byte
	h.mu.RLock()
	if len(h.data) > atIdx {
		b = h.data[atIdx]
	}
	h.mu.RUnlock()
	return b
}

func newFakeHandler() *fakeHandler {
	return &fakeHandler{
		Received: make(chan int, 100),
		data:     make([][]byte, 0, 16),
	}
}

func TestNotRateLimited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	h := newFakeHandler()
	s := httptest.NewServer(h)

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
				"prefetch_count": 0,
			},
		},
		Workers: 1,
	}

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")

	p, err := proxy.DefaultFactory(l).New(cfg.Endpoint)
	if err != nil {
		t.Errorf("cannot create default factory: %s", err.Error())
		return
	}

	// keep consuming pings from the worker, otherwise the initial ppin
	// will block in the subscriber
	pingChan := make(chan string)
	go func() {
		for {
			select {
			case <-pingChan:
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
	// blocks (it keeps running in a loop).
	var asyncAgentErr error
	go func() {
		asyncAgentErr = New(ctx, cfg, opts)
	}()

	// lets put a small wait, to let the service start
	time.Sleep(time.Second)

	if err := publish(host, exchange, 1, "hello 0"); err != nil {
		t.Errorf("cannot publish event: %s", err.Error())
	}

	timeout := time.Second * 6
	select {
	case <-time.After(timeout):
		t.Errorf("timed out at the end")
		return
	case <-h.Received:
	}

	if asyncAgentErr != nil {
		t.Errorf("async agent failed with %s", asyncAgentErr.Error())
	}
	cancel()
	// now, we do a clean shutdown

	d := h.Data(0)
	if len(d) == 0 {
		t.Errorf("empty data received")
		return
	}

	want := `{"msg":"hello 0"}`
	got := string(d)
	if got != want {
		t.Errorf("want: '%s' got '%s'", want, got)
	}
}

func TestRateLimited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	h := newFakeHandler()
	s := httptest.NewServer(h)

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
		MaxRate: 1,
	}

	buf := new(bytes.Buffer)
	l, _ := logging.NewLogger("DEBUG", buf, "")

	p, err := proxy.DefaultFactory(l).New(cfg.Endpoint)
	if err != nil {
		t.Errorf("cannot create default factory: %s", err.Error())
		return
	}

	// keep consuming pings from the worker, otherwise the initial ppin
	// will block in the subscriber
	pingChan := make(chan string)
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

	// lets put a small wait, to let the service start
	time.Sleep(time.Second)
	// the agent must be launch in a goroutine, because the New subscriber
	// blocks in a loop.
	go func() {
		if err := New(ctx, cfg, opts); err != nil {
			t.Errorf("async agent exited with an error: %s", err.Error())
			return
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			if err := publish(host, exchange, 1, fmt.Sprintf("hello %d", i)); err != nil {
				t.Errorf("cannot publish in goroutine: %s", err.Error())
			}
		}
	}()

	after := time.After(time.Second * 3)
	keepWorking := true
	numProcessed := 0
	for keepWorking {
		select {
		case <-after:
			keepWorking = false
		case <-h.Received:
			numProcessed++
		}
	}

	cancel()
	// now, we do a clean shutdown
	if numProcessed > 3 {
		t.Errorf("in 3 seconds it has processed %d events", numProcessed)
	}
}
