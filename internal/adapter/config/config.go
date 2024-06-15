package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log/slog"
	"strings"
)

// Container contains environment variables for the application, database, cache, token, and http server
type (
	Container struct {
		App           *App
		Token         *Token
		Redis         *Redis
		DB            *DB
		HTTP          *HTTP
		KafkaProducer *KafkaProducer
		KafkaConsumer *KafkaConsumer
	}

	// App contains all the environment variables for the application
	App struct {
		Name string
		Env  string
	}

	// Token contains all the environment variables for the token service
	Token struct {
		Duration string
	}

	// Redis contains all the environment variables for the cache service
	Redis struct {
		Addr     string
		Password string
	}

	// DB contains all the environment variables for the database
	DB struct {
		Host                    string
		Port                    string
		User                    string
		Password                string
		DBName                  string
		TimeZone                string
		SSL                     string
		Debug                   bool
		ConnectionMaxLifetimeMs int
		MaxIdleConnections      int
		MaxOpenConnections      int
	}

	// HTTP contains all the environment variables for the http server
	HTTP struct {
		Env            string
		URL            string
		Port           string
		AllowedOrigins string
	}

	// KafkaProducer contains all the environment variables for the kafka producer
	KafkaProducer struct {
		Brokers []string
		Topics  struct {
			Loyalty  string
			Customer string
		}
	}

	// KafkaConsumer contains all the environment variables for the kafka consumer
	KafkaConsumer struct {
		Brokers []string
		GroupID string
		Topics  struct {
			Demo    string
			Loyalty string
		}
		DeadLetterTopic string
		PoolSize        int
		Verbose         bool // Sarama logging
		KafkaVersion    string
		Assignor        string // Consumer group partition assignment strategy (values: range, roundrobin, sticky)
		Oldest          bool   // Kafka consumer consume initial offset from oldest
		MessageTTL      int64  // Message TTL is the time message live on Kafka in seconds
	}
)

// New creates a new container instance
func New() (*Container, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			slog.Warn("config file not found; ignore error if desired")
		} else {
			panic(fmt.Errorf("fatal error config file: %w", err))
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// unmarshal to config object
	var cfg = &Container{}
	if err := viper.Unmarshal(cfg); err != nil {
		panic(fmt.Errorf("Error unmarshal the configuration file: %s \n", err))
	}

	return cfg, nil
}
