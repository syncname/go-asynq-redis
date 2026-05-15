package main

import (
	"log"

	"github.com/hibiken/asynq"

	"github.com/syncname/go-asynq-redis/internal/tasks"
)

func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		asynq.Config{Concurrency: 10},
	)

	mux := asynq.NewServeMux()
	mux.Handle(tasks.TypeEmailSend, &tasks.EmailHandler{})

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
