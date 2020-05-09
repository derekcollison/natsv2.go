package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
)

type Connection interface {
	Publish(string, interface{}) error
	Subscribe(string, ...SubOption) (Subscription, error)
	Request(string, interface{}, ...ReqOption) (*nats.Msg, error)
	Handle(string, HTTPHandlerFunc) error
	Close()
}

type HTTPHandlerFunc func(http.ResponseWriter, *http.Request)

type Subscription interface {
	Close()
}

type SubOption func(*SubOptions) error

type SubOptions struct {
	Queue   string
	Handler nats.MsgHandler
}

func Queue(name string) SubOption {
	return func(o *SubOptions) error {
		o.Queue = name
		return nil
	}
}

func Handler(mcb nats.MsgHandler) SubOption {
	return func(o *SubOptions) error {
		o.Handler = mcb
		return nil
	}
}

type ReqOption func(*ReqOptions) error

type ReqOptions struct {
	Timeout time.Duration
	Context context.Context
}

func Timeout(timeout time.Duration) ReqOption {
	return func(o *ReqOptions) error {
		o.Timeout = timeout
		return nil
	}
}

func Ctx(ctx context.Context) ReqOption {
	return func(o *ReqOptions) error {
		o.Context = ctx
		return nil
	}
}

func (c *conn) Handle(subject string, handler HTTPHandlerFunc) error {
	return nil
}

func (c *conn) Request(subject string, msg interface{}, opts ...ReqOption) (*nats.Msg, error) {
	ropts := &ReqOptions{}
	for _, opt := range opts {
		if err := opt(ropts); err != nil {
			return nil, err
		}
	}
	fmt.Printf("opts are %+v\n", ropts)
	return nil, nil
}

func (c *conn) Subscribe(subject string, opts ...SubOption) (Subscription, error) {
	sopts := &SubOptions{}
	for _, opt := range opts {
		if err := opt(sopts); err != nil {
			return nil, err
		}
	}
	fmt.Printf("opts are %+v\n", sopts)
	return nil, nil
}

func (c *conn) Publish(subject string, msg interface{}) error {
	// By default we accept some things, but in the end we need []byte.
	// Will have optional helpers to do some of this.
	var data []byte
	switch v := msg.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		// My hunch is this is just as fast if not faster then doing all the
		// low level stuff directly since buf pooling.
		data = []byte(fmt.Sprintf("%+v", v))
	}
	return c.nc.Publish(subject, data)
}

func (c *conn) Close() {
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
	}
}

// For now reuse low level NATS client lib
type conn struct {
	nc *nats.Conn
}

func Connect(url string, opts ...nats.Option) (Connection, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	fmt.Printf("AAA\n\n")
	return &conn{nc: nc}, nil
}

func main() {
	nc, err := Connect("demo.nats.io")
	if err != nil {
		log.Fatalf("Could not connect: %v\n", err)
	}

	tsubj := "natsv2.foo"

	// Do basic style publish.
	nc.Publish(tsubj, "Hello World!")
	nc.Publish(tsubj, 22)

	type person struct {
		Name    string
		Address string
		Age     int
	}

	me := &person{Name: "derek", Age: 22, Address: "Los Angeles, CA"}

	nc.Publish(tsubj, me) // This will be what fmt.Printf generates.

	nc.Publish(tsubj, JSON(me))

	nc.Publish(tsubj, Base64(Gzip(JSON(me))))

	nc.Subscribe("foo")
	nc.Subscribe("foo", Queue("bar"))
	nc.Subscribe("foo", Handler(func(msg *nats.Msg) {}))

	nc.Request("service", "2+2")
	nc.Request("service", "2+2", Timeout(2*time.Second))

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	nc.Request("service", "2+2", Ctx(ctx))

	// For HTTP compatabilty. Also all middlewares etc.
	nc.Handle("foo", func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, fmt.Sprintf("Hello from NATS for %q!\n", req.URL.Path))
	})

	nc.Close()
}

func JSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func Gzip(in []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(in)
	zw.Close()
	return buf.Bytes()
}

func Base64(in []byte) []byte {
	out := make([]byte, base64.StdEncoding.EncodedLen(len(in)))
	base64.StdEncoding.Encode(out, in)
	return out
}
