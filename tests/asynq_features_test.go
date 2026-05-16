package main

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/syncname/go-asynq-redis/internal/tasks"
)

func TestSchedules_RelativeAndAbsolute(t *testing.T) {
	// Инициализируем эфемерное хранилище.
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}
	client := asynq.NewClient(opt)
	defer client.Close()
	insp := asynq.NewInspector(opt)

	// Задача 1: относительное время (ProcessIn).
	taskRelative, err := tasks.NewEmailTask(tasks.EmailPayload{To: "relative@void.local"})
	if err != nil {
		t.Fatalf("не удалось создать первую задачу: %v", err)
	}
	infoRelative, err := client.Enqueue(taskRelative, asynq.ProcessIn(30*time.Minute))
	if err != nil {
		t.Fatalf("ошибка постановки первой задачи в очередь: %v", err)
	}

	// Задача 2: абсолютное время (ProcessAt).
	taskAbsolute, err := tasks.NewEmailTask(tasks.EmailPayload{To: "absolute@void.local"})
	if err != nil {
		t.Fatalf("не удалось создать вторую задачу: %v", err)
	}
	targetDate := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	infoAbsolute, err := client.Enqueue(taskAbsolute, asynq.ProcessAt(targetDate))
	if err != nil {
		t.Fatalf("ошибка постановки второй задачи в очередь: %v", err)
	}

	// Вспомогательная функция: проверяет State=Scheduled и выводит подробности.
	// Так в логе теста сразу видно, в какой именно момент задача проснётся.
	verifyAndLog := func(name string, enqueuedInfo *asynq.TaskInfo) {
		got, err := insp.GetTaskInfo(enqueuedInfo.Queue, enqueuedInfo.ID)
		if err != nil {
			t.Fatalf("[%s] ошибка получения информации о задаче: %v", name, err)
		}
		if got.State != asynq.TaskStateScheduled {
			t.Fatalf("[%s] state = %v, want Scheduled", name, got.State)
		}
		t.Logf("=== %s ===", name)
		t.Logf("  Task ID:           %s", got.ID)
		t.Logf("  Queue:             %s", got.Queue)
		t.Logf("  Type:              %s", got.Type)
		t.Logf("  State:             %s", got.State)
		t.Logf("  NextProcessAt:     %v", got.NextProcessAt.Format(time.RFC3339))
		t.Logf("  Max Retry:         %d", got.MaxRetry)
	}

	verifyAndLog("Относительное расписание (ProcessIn 30m)", infoRelative)
	verifyAndLog("Абсолютное расписание (ProcessAt 1 June 2026)", infoAbsolute)
}
