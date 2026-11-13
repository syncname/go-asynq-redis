package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════
// 7–8. Организация очередей и эксплуатация
// ═══════════════════════════════════════════════════════════════════════════

// TestOps_PauseQueue: пауза останавливает выборку из очереди — задачи
// копятся в pending, а не сжигают попытки. После UnpauseQueue worker
// разбирает накопленное (раздел 7.3).
func TestOps_PauseQueue(t *testing.T) {
	const queue = "emails"
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	for i := 0; i < 3; i++ {
		_, err := client.Enqueue(asynq.NewTask("email:send", nil), asynq.Queue(queue))
		require.NoError(t, err)
	}
	require.NoError(t, insp.PauseQueue(queue))

	var processed atomic.Int32
	startServer(t, opt, asynq.Config{Concurrency: 1, Queues: map[string]int{queue: 1}},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			processed.Add(1)
			return nil
		}))

	require.Never(t, func() bool { return processed.Load() > 0 },
		500*time.Millisecond, 50*time.Millisecond, "из очереди на паузе ничего не должно забираться")
	q, err := insp.GetQueueInfo(queue)
	require.NoError(t, err)
	t.Logf("На паузе: paused=%v pending=%d processed=%d", q.Paused, q.Pending, processed.Load())
	require.True(t, q.Paused)
	require.Equal(t, 3, q.Pending)

	require.NoError(t, insp.UnpauseQueue(queue))
	require.Eventually(t, func() bool { return processed.Load() == 3 },
		waitTimeout, pollTick, "после снятия паузы задачи должны выполниться")
	t.Logf("После UnpauseQueue: processed=%d", processed.Load())
}

// TestOps_ArchiveAndRunAll повторяет рабочий процесс из раздела 8.4:
// задачи упали и ушли в архив → разбираем LastErr через ListArchivedTasks →
// «чиним баг» → возвращаем весь архив в работу через RunAllArchivedTasks.
func TestOps_ArchiveAndRunAll(t *testing.T) {
	const total = 3
	opt := newRedis(t)

	var fixed atomic.Bool
	var succeeded atomic.Int32
	startServer(t, opt, asynq.Config{Concurrency: 1},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			if !fixed.Load() {
				return fmt.Errorf("render template: missing field %q: %w", "name", asynq.SkipRetry)
			}
			succeeded.Add(1)
			return nil
		}))
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	for i := 0; i < total; i++ {
		_, err := client.Enqueue(asynq.NewTask("email:send", []byte(fmt.Sprintf(`{"user_id":%d}`, i))))
		require.NoError(t, err)
	}

	var archived []*asynq.TaskInfo
	require.Eventually(t, func() bool {
		list, err := insp.ListArchivedTasks("default", asynq.PageSize(100))
		if err != nil {
			return false
		}
		archived = list
		return len(list) == total
	}, waitTimeout, pollTick, "все задачи должны уйти в архив")

	for _, task := range archived {
		t.Logf("  archived: %s %s retried=%d err=%q", task.ID, task.Type, task.Retried, task.LastErr)
		require.Contains(t, task.LastErr, "missing field")
	}

	fixed.Store(true) // «задеплоили фикс»
	n, err := insp.RunAllArchivedTasks("default")
	require.NoError(t, err)
	require.Equal(t, total, n)

	require.Eventually(t, func() bool { return succeeded.Load() == total },
		waitTimeout, pollTick, "после перезапуска задачи должны выполниться")
	left, err := insp.ListArchivedTasks("default")
	require.NoError(t, err)
	t.Logf("Перезапущено %d, выполнено %d, в архиве осталось %d", n, succeeded.Load(), len(left))
	require.Empty(t, left)
}

// TestOps_ErrorHandlerSeesFinalAttempt: Config.ErrorHandler вызывается на
// каждую неудачную попытку, а из ctx можно узнать номер попытки и лимит.
// Так отличают «ещё будет retry» от «задача умерла» и обновляют статус в
// бизнес-модели только на последней попытке (разделы 8.5 и 9).
func TestOps_ErrorHandlerSeesFinalAttempt(t *testing.T) {
	opt := newRedis(t)

	type failure struct {
		retried, maxRetry int
		final             bool
	}
	failures := make(chan failure, 10)

	startServer(t, opt, asynq.Config{
		Concurrency:    1,
		RetryDelayFunc: noDelay,
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, _ *asynq.Task, err error) {
			retried, _ := asynq.GetRetryCount(ctx)
			maxRetry, _ := asynq.GetMaxRetry(ctx)
			failures <- failure{
				retried:  retried,
				maxRetry: maxRetry,
				final:    retried >= maxRetry || errors.Is(err, asynq.SkipRetry),
			}
		}),
	}, asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return errors.New("smtp: connection refused")
	}))
	client := newClient(t, opt)

	_, err := client.Enqueue(asynq.NewTask("email:send", nil), asynq.MaxRetry(2))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		f := receive(t, failures)
		t.Logf("ErrorHandler #%d: retry=%d/%d final=%v", i+1, f.retried, f.maxRetry, f.final)
		require.Equal(t, i, f.retried)
		require.Equal(t, i == 2, f.final, "финальной должна быть только третья попытка")
	}
}
