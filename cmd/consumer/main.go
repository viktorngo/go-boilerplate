package main

import (
	"context"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/adapter/handler/consumer"
	"go-boilerplate/internal/adapter/logger"
	"go-boilerplate/internal/adapter/queue/kafka"
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

	// Init database
	ctx := context.Background()

	// create consumer
	demoHandler := &consumer.DemoHandler{}
	demoConsumer, err := kafka.NewConsumer(ctx, cfg.KafkaConsumer, demoHandler.HandleKafkaMessage)
	if err != nil {
		slog.Error("Error creating kafka consumer", "error", err)
		os.Exit(1)
	}

	if err := demoConsumer.StartBlocking(); err != nil {
		slog.Error("Error starting kafka consumer", "error", err)
		os.Exit(1)
	}

	slog.Info("Stop consuming message from topic = %v", demoConsumer.Topic())

}
