package nats

import (
	nats "github.com/nats-io/nats.go"
	"go-micro.dev/v4/broker"
)

type optionsKey struct{}
type drainConnectionKey struct{}
type streamNameKey struct{}

// Options accepts nats.Options
func Options(opts nats.Options) broker.Option {
	return setBrokerOption(optionsKey{}, opts)
}

// DrainConnection will drain subscription on close
func DrainConnection() broker.Option {
	return setBrokerOption(drainConnectionKey{}, struct{}{})
}
