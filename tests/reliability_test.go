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
// 6. Надёжность обработки
// ═══════════════════════════════════════════════════════════════════════════

// RateLimitError — «ожидаемый отказ» из раздела 6.1: внешний API ответил 429
// и попросил подождать.
type RateLimitError struct{ RetryAfter time.Duration }

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %s", e.RetryAfter)
}

// TestReliability_IsFailure: если IsFailure вернул false, попытка не
// расходуется — Retried не растёт, и в статистику failed она не попадает.
// Задача с MaxRetry(1) переживает три rate limit'а подряд и выполняется.
// Для сравнения та же задача без IsFailure уходит в архив после двух попыток.
func TestReliability_IsFailure(t *testing.T) {
	const rateLimited = 3 // первые три вызова отвечают 429

	run := func(t *testing.T, withIsFailure bool) (*asynq.TaskInfo, *asynq.QueueInfo, int32) {
		opt := newRedis(t)

		cfg := asynq.Config{
			Concurrency: 1,
			RetryDelayFunc: func(n int, err error, task *asynq.Task) time.Duration {
				var rl *RateLimitError
				if errors.As(err, &rl) {
					return rl.RetryAfter // ждём столько, сколько просит API
				}
				return asynq.DefaultRetryDelayFunc(n, err, task)
			},
		}
		if withIsFailure {
			cfg.IsFailure = func(err error) bool {
				var rl *RateLimitError
				return !errors.As(err, &rl) // rate limit — не сбой
			}
		}

		var attempts atomic.Int32
		startServer(t, opt, cfg, asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			if attempts.Add(1) <= rateLimited {
				return &RateLimitError{RetryAfter: 50 * time.Millisecond}
			}
			return nil
		}))
		client := newClient(t, opt)
		insp := newInspector(t, opt)

		info, err := client.Enqueue(asynq.NewTask("webhook:send", nil),
			asynq.MaxRetry(1), asynq.Retention(time.Hour))
		require.NoError(t, err)

		want := asynq.TaskStateArchived
		if withIsFailure {
			want = asynq.TaskStateCompleted
		}
		got := waitState(t, insp, info.Queue, info.ID, want)
		q, err := insp.GetQueueInfo(info.Queue)
		require.NoError(t, err)
		t.Logf("state=%s attempts=%d retried=%d/%d failed_total=%d",
			got.State, attempts.Load(), got.Retried, got.MaxRetry, q.FailedTotal)
		return got, q, attempts.Load()
	}

	t.Run("с IsFailure", func(t *testing.T) {
		got, q, attempts := run(t, true)
		require.EqualValues(t, rateLimited+1, attempts, "три 429 и успех, хотя MaxRetry(1)")
		require.Zero(t, got.Retried, "rate limit не расходует попытки")
		require.Zero(t, q.FailedTotal, "и не засчитывается как failed")
	})

	t.Run("без IsFailure", func(t *testing.T) {
		got, q, attempts := run(t, false)
		require.EqualValues(t, 2, attempts, "1 попытка + 1 retry, дальше архив")
		require.Equal(t, 1, got.Retried)
		require.EqualValues(t, 2, q.FailedTotal)
	})
}

// TestReliability_RevokeTask: ошибка, обёрнутая в asynq.RevokeTask, — третий
// вариант исхода наряду с retry и SkipRetry. Задача не ретраится и не
// архивируется, а просто удаляется. Подходит для «срока годности» (6.2),
// если нужно не засорять архив, но всё же вернуть причину в ErrorHandler.
func TestReliability_RevokeTask(t *testing.T) {
	opt := newRedis(t)

	var attempts atomic.Int32
	startServer(t, opt, asynq.Config{Concurrency: 1},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			attempts.Add(1)
			return fmt.Errorf("task expired: %w", asynq.RevokeTask)
		}))
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	info, err := client.Enqueue(asynq.NewTask("email:followup", nil), asynq.MaxRetry(5))
	require.NoError(t, err)

	waitDeleted(t, insp, info.Queue, info.ID)
	archived, err := insp.ListArchivedTasks(info.Queue)
	require.NoError(t, err)
	t.Logf("attempts=%d, в архиве: %d", attempts.Load(), len(archived))

	require.EqualValues(t, 1, attempts.Load())
	require.Empty(t, archived, "RevokeTask не кладёт задачу в архив")
}
