package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"github.com/syncname/go-asynq-redis/internal/tasks"
)

// ═══════════════════════════════════════════════════════════════════════════
// 4. Первый рабочий пример: задача, handler, жизненный цикл
// ═══════════════════════════════════════════════════════════════════════════

// TestEmailTask_Constructor: конструктор из internal/tasks собирает задачу
// с нужным типом и JSON-payload, который handler разберёт обратно.
// Сетевых вызовов здесь нет — Redis не нужен.
func TestEmailTask_Constructor(t *testing.T) {
	p := tasks.EmailPayload{UserID: 42, To: "alice@example.com", Subject: "Welcome", Body: "Hi!"}
	task, err := tasks.NewEmailTask(p)
	require.NoError(t, err)

	require.Equal(t, tasks.TypeEmailSend, task.Type())
	t.Logf("type=%s payload=%s", task.Type(), task.Payload())

	var decoded tasks.EmailPayload
	require.NoError(t, json.Unmarshal(task.Payload(), &decoded))
	require.Equal(t, p, decoded)
}

// TestEmailTask_HandlerContract проверяет контракт handler'а из раздела 4:
// корректный payload — nil (задача выполнена), битый — ошибка, обёрнутая
// в SkipRetry (задача сразу уйдёт в архив). Handler — обычная функция,
// поэтому его можно вызвать напрямую, без сервера и Redis.
func TestEmailTask_HandlerContract(t *testing.T) {
	h := &tasks.EmailHandler{}

	valid, err := tasks.NewEmailTask(tasks.EmailPayload{To: "alice@example.com"})
	require.NoError(t, err)
	require.NoError(t, h.ProcessTask(context.Background(), valid))

	broken := asynq.NewTask(tasks.TypeEmailSend, []byte(`{"to": 42`))
	err = h.ProcessTask(context.Background(), broken)
	t.Logf("Битый payload → %v", err)
	require.ErrorIs(t, err, asynq.SkipRetry, "битый payload не должен ретраиться")
}

// TestLifecycle_Defaults: что Asynq проставляет задаче без опций.
// Очередь "default", 25 попыток, таймаут 30 минут, без дедлайна и без
// хранения после выполнения.
func TestLifecycle_Defaults(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)

	task, err := tasks.NewEmailTask(tasks.EmailPayload{To: "alice@example.com"})
	require.NoError(t, err)
	info, err := client.Enqueue(task)
	require.NoError(t, err)

	t.Logf("queue=%s state=%s max_retry=%d timeout=%v deadline=%v retention=%v",
		info.Queue, info.State, info.MaxRetry, info.Timeout, info.Deadline, info.Retention)
	require.Equal(t, "default", info.Queue)
	require.Equal(t, asynq.TaskStatePending, info.State)
	require.Equal(t, 25, info.MaxRetry)
	require.Equal(t, 30*time.Minute, info.Timeout)
	require.True(t, info.Deadline.IsZero())
	require.Zero(t, info.Retention)
}

// TestLifecycle_PendingActiveCompleted проходит нормальный путь задачи
// pending → active → completed и проверяет, что completed бывает только
// с Retention.
func TestLifecycle_PendingActiveCompleted(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	// Worker ещё не запущен — задача ждёт в pending.
	info, err := client.Enqueue(asynq.NewTask("test:step", nil), asynq.Retention(time.Hour))
	require.NoError(t, err)
	got, err := insp.GetTaskInfo(info.Queue, info.ID)
	require.NoError(t, err)
	t.Logf("1. после Enqueue:      %s", got.State)
	require.Equal(t, asynq.TaskStatePending, got.State)

	// Handler держит задачу, пока тест не отпустит её, — так active видно
	// в Inspector, а не только в теории.
	release := make(chan struct{})
	startServer(t, opt, asynq.Config{Concurrency: 1},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			<-release
			return nil
		}))

	got = waitState(t, insp, info.Queue, info.ID, asynq.TaskStateActive)
	t.Logf("2. handler работает:   %s", got.State)
	close(release)

	got = waitState(t, insp, info.Queue, info.ID, asynq.TaskStateCompleted)
	t.Logf("3. handler вернул nil: %s, completed_at=%s", got.State, got.CompletedAt.Format(time.RFC3339))
	require.False(t, got.CompletedAt.IsZero())
	require.Equal(t, time.Hour, got.Retention)
}

// TestLifecycle_DeletedWithoutRetention: без Retention выполненная задача
// удаляется из Redis сразу, и Inspector отвечает ErrTaskNotFound — ровно то,
// что обрабатывает cmd/inspector из статьи. Handler — настоящий EmailHandler.
func TestLifecycle_DeletedWithoutRetention(t *testing.T) {
	opt := newRedis(t)

	mux := asynq.NewServeMux()
	mux.Handle(tasks.TypeEmailSend, &tasks.EmailHandler{})
	startServer(t, opt, asynq.Config{Concurrency: 1}, mux)

	client := newClient(t, opt)
	insp := newInspector(t, opt)

	task, err := tasks.NewEmailTask(tasks.EmailPayload{UserID: 42, To: "alice@example.com", Subject: "Welcome"})
	require.NoError(t, err)
	info, err := client.Enqueue(task)
	require.NoError(t, err)

	waitDeleted(t, insp, info.Queue, info.ID)
	_, err = insp.GetTaskInfo(info.Queue, info.ID)
	t.Logf("GetTaskInfo после выполнения: %v", err)
	require.ErrorIs(t, err, asynq.ErrTaskNotFound)
}
