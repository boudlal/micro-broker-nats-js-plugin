// Package nats provides a NATS broker with JetStream support
package nats

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/codec/json"
	"go-micro.dev/v4/logger"
	"go-micro.dev/v4/registry"
	"go-micro.dev/v4/util/cmd"
)

func init() {
	cmd.DefaultBrokers["nats"] = NewBroker
}

type natsBroker struct {
	sync.Once
	sync.RWMutex

	connected bool
	addrs     []string
	conn      *nats.Conn
	js        nats.JetStreamContext
	opts      broker.Options
	nopts     nats.Options
}

type subscriber struct {
	subscription *nats.Subscription
	topic        string
	opts         broker.SubscribeOptions
}

type publication struct {
	t   string
	err error
	m   *broker.Message
}

func (p *publication) Topic() string {
	return p.t
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) Ack() error {
	// Acknowledgment handled automatically in JetStream
	return nil
}

func (p *publication) Error() error {
	return p.err
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe() error {
	return s.subscription.Unsubscribe()
}

func (n *natsBroker) Address() string {
	if n.conn != nil && n.conn.IsConnected() {
		return n.conn.ConnectedUrl()
	}
	if len(n.addrs) > 0 {
		return n.addrs[0]
	}
	return ""
}

func (n *natsBroker) Connect() error {
	n.Lock()
	defer n.Unlock()

	if n.connected {
		return nil
	}

	opts := n.nopts
	opts.Servers = n.addrs
	opts.Secure = n.opts.Secure
	opts.TLSConfig = n.opts.TLSConfig

	if n.opts.TLSConfig != nil {
		opts.Secure = true
	}

	conn, err := opts.Connect()
	if err != nil {
		return err
	}

	js, err := conn.JetStream()
	if err != nil {
		return err
	}

	n.conn = conn
	n.js = js
	n.connected = true

	// Add a default stream if it doesn't exist
	streamName := "micro_stream"
	if sName, ok := n.opts.Context.Value(streamNameKey{}).(string); ok && sName != "" {
		streamName = sName
	}
	subject := streamName + ".*"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: nats.LimitsPolicy,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return err
	}
	return nil
}

func (n *natsBroker) Disconnect() error {
	n.Lock()
	defer n.Unlock()

	if n.conn != nil {
		n.conn.Close()
	}
	n.connected = false
	return nil
}

func (n *natsBroker) Init(opts ...broker.Option) error {
	n.setOption(opts...)
	return nil
}

func (n *natsBroker) Options() broker.Options {
	return n.opts
}

func (n *natsBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	n.RLock()
	defer n.RUnlock()

	if n.js == nil {
		return errors.New("JetStream is not initialized")
	}

	data, err := n.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = n.js.Publish(topic, data)
	return err
}

func (n *natsBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	n.RLock()
	if n.js == nil {
		n.RUnlock()
		return nil, errors.New("JetStream is not initialized")
	}
	n.RUnlock()

	opt := broker.SubscribeOptions{
		AutoAck: true,
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&opt)
	}

	consumer := func(msg *nats.Msg) {
		var m broker.Message
		pub := &publication{t: topic}
		eh := n.opts.ErrorHandler

		if err := n.opts.Codec.Unmarshal(msg.Data, &m); err != nil {
			pub.err = err
			pub.m = &m
			if logger.V(logger.ErrorLevel, logger.DefaultLogger) {
				logger.Error(err)
			}
			if eh != nil {
				eh(pub)
			}
			return
		}

		pub.m = &m
		if err := handler(pub); err != nil {
			pub.err = err
			if logger.V(logger.ErrorLevel, logger.DefaultLogger) {
				logger.Error(err)
			}
			if eh != nil {
				eh(pub)
			}
		}
	}

	durableName := "durable-" + strings.ReplaceAll(topic, ".", "")
	sub, err := n.js.Subscribe(topic, consumer, nats.Durable(durableName))
	if err != nil {
		return nil, err
	}

	return &subscriber{
		subscription: sub,
		topic:        topic,
		opts:         opt,
	}, nil
}

func (n *natsBroker) String() string {
	return "nats"
}

func (n *natsBroker) setOption(opts ...broker.Option) {
	for _, o := range opts {
		o(&n.opts)
	}

	n.Once.Do(func() {
		n.nopts = nats.GetDefaultOptions()
	})

	if nopts, ok := n.opts.Context.Value(optionsKey{}).(nats.Options); ok {
		n.nopts = nopts
	}

	n.addrs = n.setAddrs(n.opts.Addrs)
}

func (n *natsBroker) setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		Codec:    json.Marshaler{},
		Context:  context.Background(),
		Registry: registry.DefaultRegistry,
	}

	n := &natsBroker{
		opts: options,
	}
	n.setOption(opts...)

	return n
}
