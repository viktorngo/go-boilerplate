package main

import (
	"context"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/adapter/handler/consumer"
	"go-boilerplate/internal/adapter/logger"
	"go-boilerplate/internal/adapter/queue/kafka"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		slog.Error("Error loading environment variables", "error", err)
		os.Exit(1)
	}

	// Set logger
	logger.Set(cfg.App)

	slog.Info("Starting the application", "app", cfg.App.Name, "env", cfg.App.Env)

	// Init database
	ctx := context.Background()

	// create consumer
	demoHandler := &consumer.DemoHandler{}
	loyaltyHandler := &consumer.LoyaltyHandler{}

	topicHandlerMap := map[string]kafka.MessageHandler{
		cfg.KafkaConsumer.Topics.Demo:    demoHandler.HandleKafkaMessage,
		cfg.KafkaConsumer.Topics.Loyalty: loyaltyHandler.HandleKafkaMessage,
	}
	kafkaConsumer, err := kafka.NewConsumer(ctx, cfg.KafkaConsumer, topicHandlerMap)
	if err != nil {
		slog.Error("Error creating kafka consumer", "error", err)
		os.Exit(1)
	}

	// Create a channel to listen for signals
	done := make(chan bool)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := kafkaConsumer.ConsumeMultipleTopics(ctx); err != nil {
			slog.Error("Error starting kafka consumer", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for signal and then initiate shutdown
	<-signals
	slog.Info("Received shutdown signal")

	// Stop consumer
	kafkaConsumer.Stop()

	// Signal to main routine that shutdown is complete
	close(done)

	slog.Info("Stopped consumer")

	// Wait for server to finish shutting down
	<-done

}
