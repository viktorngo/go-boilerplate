package kafka

import (
	"context"
	"errors"
	"github.com/goccy/go-json"
	"github.com/segmentio/kafka-go"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/core/port"
	"log/slog"
	"time"
)

type MessageHandler func(msg kafka.Message) error

type Consumer struct {
	cfg            *config.KafkaConsumer
	messageHandler MessageHandler
	reader         *kafka.Reader
	pool           *WorkerPool
	cancelFnc      context.CancelFunc
	deadLetter     *kafka.Writer
	pause          bool
}

func NewConsumer(ctx context.Context, cfg *config.KafkaConsumer, msgHandler MessageHandler) (port.Consumer, error) {
	if msgHandler == nil {
		return nil, errors.New("kafka: Message handler is not set")
	}

	consumer := &Consumer{
		cfg:            cfg,
		messageHandler: msgHandler,
		pool:           NewWorkerPoolWithSize(cfg.PoolSize),
	}

	if cfg.PoolSize == 0 {
		consumer.pool = NewWorkerPool()
	} else {
		consumer.pool = NewWorkerPoolWithSize(cfg.PoolSize)
	}

	// create dead letter topic
	if err := consumer.registerDeadLetterTopic(ctx); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (consumer *Consumer) Topic() string {
	return consumer.cfg.Topic
}

// StartBlocking start kafka reader and consume the message. This is blocking method
func (consumer *Consumer) StartBlocking() error {

	ctx, cancelFnc := context.WithCancel(context.Background())
	consumer.cancelFnc = cancelFnc
	slog.Info("Start consume message", "topic", consumer.cfg.Topic)

	kafkaConfig := kafka.ReaderConfig{
		CommitInterval: time.Second,
		Brokers:        consumer.cfg.Brokers,
		GroupID:        consumer.cfg.GroupID,
		Topic:          consumer.cfg.Topic,
	}
	consumer.reader = kafka.NewReader(kafkaConfig)
	consumer.reader.Grou

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if !consumer.pause {
				consumer.consume(ctx)
			}
		}
	}
}

func (consumer *Consumer) Pause() {
	consumer.pause = true
}

func (consumer *Consumer) Resume() {
	consumer.pause = false
}

// Stop the consumer and close dead letter
func (consumer *Consumer) Stop() {
	// Need to wait all message to be finished before stop otherwise it will cause panic
	defer consumer.pool.Wait()

	// run cancel function
	if consumer.cancelFnc != nil {
		consumer.cancelFnc()
	}

	// send message to dead letter
	if consumer.deadLetter == nil {
		return
	}
	if err := consumer.deadLetter.Close(); err != nil {
		slog.Error("close dead letter producer", "error", err)
	}
}

// consume the message using worker pool and send it to message handler
func (consumer *Consumer) consume(ctx context.Context) {
	//! When using ReadMessage -> if GroupID is specific then it will AutoCommit Message, otherwise it only fetch and we need commit manually
	msg, err := consumer.reader.ReadMessage(ctx)
	if err != nil {
		slog.Error("Kafka: cannot fetch message", "error", err)
		return
	}

	consumer.pool.Acquire()
	go func(ctx context.Context, msg kafka.Message) {
		defer consumer.pool.Release()

		// handle the message
		if err := consumer.messageHandler(msg); err != nil {
			if consumer.deadLetter != nil {
				go consumer.handleErrorMessage(ctx, msg, err)
			}
		}

	}(ctx, msg)
}

func (consumer *Consumer) handleErrorMessage(ctx context.Context, msg kafka.Message, processErr error) {
	emsg, _ := json.Marshal(ErrorMessage{
		OriginPartition: msg.Partition,
		Topic:           msg.Topic,
		OriginOffset:    msg.Offset,
		Value:           msg.Value,
		Error:           processErr.Error(),
	})

	err := consumer.deadLetter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: emsg,
	})
	if err != nil {
		slog.Error("write dead letter", "error", err)
	}
}

func (consumer *Consumer) registerDeadLetterTopic(ctx context.Context) error {
	if consumer.cfg.DeadLetterTopic == "" {
		return nil
	}

	slog.Info("Register dead letter", "topic", consumer.cfg.DeadLetterTopic, "broker", consumer.cfg.Brokers)
	consumer.deadLetter = &kafka.Writer{
		Addr:        kafka.TCP(consumer.cfg.Brokers...),
		Topic:       consumer.cfg.DeadLetterTopic,
		MaxAttempts: 3,
	}
	slog.Info("register dead letter success", "topic", consumer.cfg.DeadLetterTopic)
	return nil
}
