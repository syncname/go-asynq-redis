// internal/tasks/email.go
package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

type EmailHandler struct{}

const TypeEmailSend = "email:send"

type EmailPayload struct {
	UserID  int64  `json:"user_id"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func NewEmailTask(p EmailPayload) (*asynq.Task, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeEmailSend, data), nil
}

func (h *EmailHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		// Битый payload — повторять бессмысленно.
		return fmt.Errorf("unmarshal: %v: %w", err, asynq.SkipRetry)
	}
	log.Printf("sending email to %s", p.To)
	// Здесь — реальная отправка через SMTP/API.
	return nil
}
