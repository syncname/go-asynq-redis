package main

import (
	"fmt"
	"log"
	"os"

	"github.com/hibiken/asynq"
)

func main() {
	insp := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})

	queues, err := insp.Queues()
	if err != nil {
		log.Fatal(err)
	}

	if len(queues) == 0 {
		fmt.Println("No queues found")
		return
	}

	fmt.Println("Queues")
	for _, q := range queues {
		fmt.Printf("- %s\n", q)
	}
	fmt.Println()

	for _, queue := range queues {
		fmt.Printf("Queue: %s\n", queue)

		pendingTasks, err := insp.ListPendingTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing pending tasks: %v", err)
		} else {
			fmt.Printf("Pending (%d):\n", len(pendingTasks))
			for _, t := range pendingTasks {
				fmt.Printf("  ID: %s | Type: %s | Payload: %s\n", t.ID, t.Type, string(t.Payload))
			}
		}

		activeTasks, err := insp.ListActiveTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing active tasks: %v", err)
		} else {
			fmt.Printf("Active (%d):\n", len(activeTasks))
			for _, t := range activeTasks {
				fmt.Printf("  ID: %s | Type: %s | Payload: %s\n", t.ID, t.Type, string(t.Payload))
			}
		}

		scheduledTasks, err := insp.ListScheduledTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing scheduled tasks: %v", err)
		} else {
			fmt.Printf("Scheduled (%d):\n", len(scheduledTasks))
			for _, t := range scheduledTasks {
				fmt.Printf("  ID: %s | Type: %s | Next process: %s\n", t.ID, t.Type, t.NextProcessAt)
			}
		}

		retryTasks, err := insp.ListRetryTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing retry tasks: %v", err)
		} else {
			fmt.Printf("Retry (%d):\n", len(retryTasks))
			for _, t := range retryTasks {
				fmt.Printf("  ID: %s | Type: %s | Next retry: %s | Retries: %d\n", t.ID, t.Type, t.NextProcessAt, t.Retried)
			}
		}

		archivedTasks, err := insp.ListArchivedTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing archived tasks: %v", err)
		} else {
			fmt.Printf("Archived (%d):\n", len(archivedTasks))
			for _, t := range archivedTasks {
				fmt.Printf("  ID: %s | Type: %s | Last failure: %s\n", t.ID, t.Type, t.LastFailedAt)
			}
		}

		completedTasks, err := insp.ListCompletedTasks(queue, "", 10, 0)
		if err != nil {
			log.Printf("Error listing completed tasks: %v", err)
		} else {
			fmt.Printf("Completed (%d):\n", len(completedTasks))
			for _, t := range completedTasks {
				fmt.Printf("  ID: %s | Type: %s | Completed at: %s\n", t.ID, t.Type, t.CompletedAt)
			}
		}

		fmt.Println()
	}

	if len(os.Args) > 1 {
		taskID := os.Args[1]
		fmt.Printf("Task Info: %s \n", taskID)

		for _, queue := range queues {
			info, err := insp.GetTaskInfo(queue, taskID)
			if err != nil {
				continue
			}
			fmt.Printf("Queue: %s\n", queue)
			fmt.Printf("State: %s\n", info.State)
			fmt.Printf("Type: %s\n", info.Type)
			fmt.Printf("Payload: %s\n", string(info.Payload))
			if !info.CompletedAt.IsZero() {
				fmt.Printf("Completed at: %s\n", info.CompletedAt)
			}
			if !info.NextProcessAt.IsZero() {
				fmt.Printf("Next process at: %s\n", info.NextProcessAt)
			}
			if info.Retried > 0 {
				fmt.Printf("Retried: %d\n", info.Retried)
			}
			if info.LastErr != "" {
				fmt.Printf("Last error: %s\n", info.LastErr)
			}
			fmt.Printf("Last failed at: %s\n", info.LastFailedAt)
			return
		}
		fmt.Printf("Task %s not found in any queue\n", taskID)
	}
}
