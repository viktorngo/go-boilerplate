package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/adapter/handler/consumer"
	"go-boilerplate/internal/adapter/logger"
	"go-boilerplate/internal/adapter/queue/kafka"
	"go-boilerplate/internal/adapter/storage/redis"
	"log/slog"
	"os"
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

	ctx := context.Background()

	// init cache
	cache, err := redis.New(ctx, cfg.Redis)
	if err != nil {
		slog.Error("Error creating cache", "error", err)
		return
	}

	// init handlers
	demoHandler := consumer.DemoHandler{}
	loyaltyHandler := consumer.LoyaltyHandler{}

	// run kafka client
	handlers := map[string]kafka.MessageHandler{
		cfg.KafkaConsumer.Topics.Demo:    demoHandler.HandleKafkaMessage,
		cfg.KafkaConsumer.Topics.Loyalty: loyaltyHandler.HandleKafkaMessage,
	}

	if err := kafka.ServeConsumerGroup(ctx, cfg.KafkaConsumer, cache, handlers); err != nil {
		slog.Error("Error creating consumer group", "error", err)
		return
	}

	slog.Info("Consumer group is stopped")
}
