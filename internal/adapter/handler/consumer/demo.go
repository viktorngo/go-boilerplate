package consumer

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/goccy/go-json"
	"log/slog"
	"time"
)

type DemoHandler struct {
	//db
}

func (s *DemoHandler) HandleKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	slog.Info("processing message from demo", "message", string(msg.Value))
	time.Sleep(10 * time.Second)
	content := make(map[string]interface{})
	if err := json.Unmarshal(msg.Value, &content); err != nil {
		return err
	}
	//slog.Info("processing message", "content", content)

	if status, ok := content["status"]; ok {
		if status == "error" {
			return errors.New("error status")
		}
	}

	return nil
}
