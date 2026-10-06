package listener

import (
	"context"

	"x2kafka/pkg/kafka"
)

type Listener interface {
	Name() string
	Start(ctx context.Context, msgChan chan<- *kafka.Message) error
	Stop() error
}
