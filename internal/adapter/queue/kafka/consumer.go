package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/core/domain/custom_errors"
	"go-boilerplate/internal/core/port"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type MessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	handlers map[string]MessageHandler
	cache    port.CacheRepository
	cfg      *config.KafkaConsumer
}

func ServeConsumerGroup(ctx context.Context, cfg *config.KafkaConsumer, cache port.CacheRepository, handlers map[string]MessageHandler) error {
	if cfg == nil {
		return errors.New("missing config")
	}
	if cache == nil {
		return errors.New("cache repository is required")
	}
	if len(handlers) == 0 {
		return errors.New("at least one handler is required")
	}

	keepRunning := true
	slog.Info("Starting a new Sarama consumer")

	if cfg.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if cfg.KafkaVersion == "" {
		cfg.KafkaVersion = sarama.DefaultVersion.String()
	}
	version, err := sarama.ParseKafkaVersion(cfg.KafkaVersion)
	if err != nil {
		return fmt.Errorf("error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = version

	switch cfg.Assignor {
	case "sticky":
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		return fmt.Errorf("unrecognized consumer group partition assignor: %s", cfg.Assignor)
	}

	if cfg.Oldest {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:    make(chan bool),
		handlers: handlers,
		cache:    cache,
		cfg:      cfg,
	}

	ctx, cancel := context.WithCancel(ctx)

	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaCfg)
	if err != nil {
		cancel()
		return fmt.Errorf("error creating consumer group client: %w", err)
	}

	// collect topics to consume
	topics := make([]string, 0, len(consumer.handlers))
	for topic := range consumer.handlers {
		topics = append(topics, topic)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					slog.Info("Consumer group has been closed")
					return
				}
				slog.Error("Error from consumer", "error", err)
				return
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	slog.Info("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			slog.Info("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			slog.Info("Received SIGTERM, shutting down")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		return fmt.Errorf("error closing client: %v", err)
	}

	return nil
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				slog.Error("message channel was closed")
				return nil
			}
			slog.Info("Message claimed", "timestamp", message.Timestamp, "topic", message.Topic, "value", message.Value)

			msgKey := fmt.Sprintf("%s-%d-%s", message.Topic, message.Offset, message.Key)
			processedMsg, err := consumer.cache.Get(session.Context(), msgKey)
			if err != nil {
				if !errors.Is(err, &custom_errors.NotfoundInCacheErr{}) {
					return fmt.Errorf("failed to get cache %w", err)
				}
			}
			if processedMsg == "processed" {
				continue
			}

			if handler, ok := consumer.handlers[message.Topic]; ok {
				if err := handler(session.Context(), message); err != nil {
					slog.Error("Error handling message", "error", err)
					// handle failed message, push to dead letter queue, etc.

					// don't mark the message as processed if push to dead letter queue is failed
					//continue
				}
				// mark the message as processed in cache
				err := consumer.cache.Set(session.Context(), msgKey, "processed", time.Duration(consumer.cfg.MessageTTL)*time.Second)
				if err != nil {
					return err
				}
				session.MarkMessage(message, "")
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
