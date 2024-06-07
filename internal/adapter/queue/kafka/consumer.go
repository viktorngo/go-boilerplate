package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/segmentio/kafka-go"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/core/port"
	"log/slog"
	"os"
	"time"
)

type MessageHandler func(msg kafka.Message) error

type Consumer struct {
	cfg             *config.KafkaConsumer
	messageHandler  MessageHandler
	reader          *kafka.Reader
	pool            *WorkerPool
	cancelFnc       context.CancelFunc
	deadLetter      *kafka.Writer
	pause           bool
	topicHandlerMap map[string]MessageHandler
}

func NewConsumer(ctxBg context.Context, cfg *config.KafkaConsumer, topicHandlerMap map[string]MessageHandler) (port.Consumer, error) {
	ctx, cancel := context.WithCancel(ctxBg)
	consumer := &Consumer{
		cfg:             cfg,
		topicHandlerMap: topicHandlerMap,
		cancelFnc:       cancel,
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

func (consumer *Consumer) Topics() []string {
	topics := make([]string, 0, len(consumer.topicHandlerMap))
	for topic := range consumer.topicHandlerMap {
		topics = append(topics, topic)
	}
	return topics
}

func (consumer *Consumer) ConsumeMultipleTopics(ctx context.Context) error {
	if len(consumer.topicHandlerMap) == 0 {
		return errors.New("no topic handler found, required at least one topic handler")
	}
	// collect all topics
	topics := make([]string, 0, len(consumer.topicHandlerMap))
	for topic := range consumer.topicHandlerMap {
		topics = append(topics, topic)
	}

	// create consumer group
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      consumer.cfg.GroupID,
		Brokers: consumer.cfg.Brokers,
		Topics:  topics,
	})
	if err != nil {
		fmt.Printf("error creating consumer group: %+v\n", err)
		os.Exit(1)
	}
	defer group.Close()

	for {
		gen, err := group.Next(ctx)
		if err != nil {
			return fmt.Errorf("error getting generation: %w", err)
		}

		for topic, assignments := range gen.Assignments {
			for _, assignment := range assignments {
				partition, offset := assignment.ID, assignment.Offset
				gen.Start(func(ctx context.Context) {
					// create reader for this partition.
					reader := kafka.NewReader(kafka.ReaderConfig{
						Brokers:   consumer.cfg.Brokers,
						Topic:     topic,
						Partition: partition,
					})
					defer reader.Close()

					// seek to the last committed offset for this partition.
					reader.SetOffset(offset)
					for {
						select {
						case <-ctx.Done():
							return
						default:
							if !consumer.pause {
								msg, err := reader.ReadMessage(ctx)
								if err != nil {
									if errors.Is(err, kafka.ErrGenerationEnded) {
										// generation has ended.  commit offsets.
										// in a real app, offsets would be committed periodically.
										if err := gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}}); err != nil {
											slog.Error("error committing offsets", "error", err)
										}
										slog.Info("generation ended", "topic", topic, "partition", partition)
										return
									}

									slog.Error("error reading message", "error", err)
									return
								}

								consumer.pool.Acquire()
								go func(ctx context.Context, msg kafka.Message) {
									defer consumer.pool.Release()

									// handle message logic
									if handler, ok := consumer.topicHandlerMap[topic]; ok {
										if err := handler(msg); err != nil {
											consumer.handleErrorMessage(ctx, msg, err)
										}
									}
									// commit the offset
									//if err := gen.CommitOffsets(map[string]map[int]int64{topic: {partition: offset + 1}}); err != nil {
									//	slog.Error("error committing offsets", "error", err)
									//}

									// handle the message
									if err := consumer.messageHandler(msg); err != nil {
										if consumer.deadLetter != nil {
											go consumer.handleErrorMessage(ctx, msg, err)
										}
									}

								}(ctx, msg)
								offset = msg.Offset

							}
						}

					}
				})
			}
		}
	}
}

// StartBlocking start kafka reader and consume the message. This is blocking method
func (consumer *Consumer) StartBlocking() error {
	ctx, cancelFnc := context.WithCancel(context.Background())
	consumer.cancelFnc = cancelFnc
	//slog.Info("Start consume message", "topic", consumer.cfg.Topic)

	kafkaConfig := kafka.ReaderConfig{
		CommitInterval: time.Second,
		Brokers:        consumer.cfg.Brokers,
		GroupID:        consumer.cfg.GroupID,
		//Topic:          consumer.cfg.Topic,
	}
	consumer.reader = kafka.NewReader(kafkaConfig)

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
	slog.Error("Kafka: handle error message", "error", processErr)
	emsg, err := json.Marshal(ErrorMessage{
		OriginPartition: msg.Partition,
		Topic:           msg.Topic,
		OriginOffset:    msg.Offset,
		Error:           processErr.Error(),
	})
	if err != nil {
		slog.Error("Kafka: can't marshal error message", "error", err)
	}

	err = consumer.deadLetter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: emsg,
	})
	if err != nil {
		slog.Error("Kafka: write dead letter", "error", err)
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
