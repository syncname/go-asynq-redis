package main

import (
	"log"

	"github.com/hibiken/asynq"
	"github.com/syncname/go-asynq-redis/internal/tasks"
)

func main() {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	defer client.Close()

	task, _ := tasks.NewEmailTask(tasks.EmailPayload{
		UserID: 42, To: "alice@example.com", Subject: "Welcome", Body: "Hi!",
	})

	info, err := client.Enqueue(
		task,
		// asynq.Retention(24*time.Hour),
	)
	if err != nil {
		go log.Fatal(err)
	}
	log.Printf("enqueued: id=%s queue=%s", info.ID, info.Queue)
}
