package postgres

import (
	"context"
	"fmt"
	"go-boilerplate/internal/adapter/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"time"
)

func New(ctx context.Context, cfg *config.DB) (*gorm.DB, func() error, error) {
	gormCfg := gorm.Config{}

	if cfg.Debug {
		gormCfg.Logger = logger.Default.LogMode(logger.Info)
	}

	gormCfg.SkipDefaultTransaction = true

	dsn := "host=%s user=%s password=%s dbname=%s port=%v sslmode=%s TimeZone=%s"
	db, err := gorm.Open(postgres.Open(fmt.Sprintf(dsn, cfg.Host, cfg.User, cfg.Password, cfg.DBName, cfg.Port, cfg.SSL, cfg.TimeZone)), &gormCfg)
	if err != nil {
		return nil, nil, err
	}

	// get sql.DB
	pgDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// Config
	pgDB.SetConnMaxLifetime(time.Duration(cfg.ConnectionMaxLifetimeMs) * time.Millisecond)
	pgDB.SetMaxIdleConns(cfg.MaxIdleConnections)
	pgDB.SetMaxOpenConns(cfg.MaxOpenConnections)

	// ping to db to test connection
	if err := pgDB.Ping(); err != nil {
		return nil, nil, err
	}

	return db, func() error {
		return pgDB.Close()
	}, nil
}
