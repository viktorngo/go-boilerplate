package port

import "context"

type Producer interface {
	// Publish sends a message to the queue
	Publish(ctx context.Context, message []byte) error
	// Close closes the connection to the queue server
	Close() error
}

type Consumer interface {
	StartBlocking() error
	Stop()
	ConsumeMultipleTopics(ctx context.Context) error
	//Topics() []string
	Pause()
	Resume()
}
