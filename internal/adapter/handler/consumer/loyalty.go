package consumer

import (
	"errors"
	"github.com/goccy/go-json"
	"github.com/segmentio/kafka-go"
	"log/slog"
)

type LoyaltyHandler struct {
}

func (s *LoyaltyHandler) HandleKafkaMessage(msg kafka.Message) error {
	slog.Info("processing message from loyalty", "message", string(msg.Value))
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