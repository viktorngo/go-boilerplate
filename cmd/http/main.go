package main

import (
	"go-boilerplate/internal/adapter/config"
	"go-boilerplate/internal/adapter/logger"
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
	//ctx := context.Background()
	//db, closeDB, err := postgres.New(ctx, cfg.DB)
	//if err != nil {
	//	slog.Error("Error initializing database connection", "error", err)
	//	os.Exit(1)
	//}
	//defer closeDB()
	//slog.Info("Successfully connected to the database", "host", cfg.DB.Host, "db", cfg.DB.DBName)

	// Migrate database
	//err = db.Migrate()
	//if err != nil {
	//	slog.Error("Error migrating database", "error", err)
	//	os.Exit(1)
	//}
	//
	//slog.Info("Successfully migrated the database")

	// Init cache service
	//cache, err := redis.New(ctx, cfg.Redis)
	//if err != nil {
	//	slog.Error("Error initializing cache connection", "error", err)
	//	os.Exit(1)
	//}
	//defer cache.Close()

	//slog.Info("Successfully connected to the cache server")

	// Init token service
	//token, err := paseto.New(config.Token)
	//if err != nil {
	//	slog.Error("Error initializing token service", "error", err)
	//	os.Exit(1)
	//}
}
