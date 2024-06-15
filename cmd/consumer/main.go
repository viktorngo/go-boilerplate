package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/adapter/handler/consumer"
	"go-boilerplate/internal/adapter/logger"
	"go-boilerplate/internal/adapter/queue/saram"
	"go-boilerplate/internal/adapter/storage/redis"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
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

	ctx, cancel := context.WithCancel(context.Background())
	keepRunning := true
	consumptionIsPaused := false
	wg := &sync.WaitGroup{}

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
	handlers := map[string]saram.MessageHandler{
		cfg.KafkaConsumer.Topics.Demo:    demoHandler.HandleKafkaMessage,
		cfg.KafkaConsumer.Topics.Loyalty: loyaltyHandler.HandleKafkaMessage,
	}

	client, err := saram.NewConsumerGroup(ctx, wg, cfg.KafkaConsumer, cache, handlers)
	if err != nil {
		slog.Error("Error creating consumer group", "error", err)
		return
	}

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
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
