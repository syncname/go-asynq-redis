package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

// Общие помощники. Каждый тест поднимает собственный miniredis, поэтому тесты
// не делят состояние между собой и не требуют настоящего Redis.

const (
	waitTimeout = 10 * time.Second      // потолок ожидания асинхронных переходов
	pollTick    = 20 * time.Millisecond // как часто опрашиваем Inspector
)

// newRedis запускает in-process Redis и возвращает опции подключения к нему.
// miniredis.RunT сам остановит сервер через t.Cleanup.
func newRedis(t *testing.T) asynq.RedisClientOpt {
	t.Helper()
	mr := miniredis.RunT(t)
	return asynq.RedisClientOpt{Addr: mr.Addr()}
}

func newClient(t *testing.T, opt asynq.RedisClientOpt) *asynq.Client {
	t.Helper()
	client := asynq.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newInspector(t *testing.T, opt asynq.RedisClientOpt) *asynq.Inspector {
	t.Helper()
	insp := asynq.NewInspector(opt)
	t.Cleanup(func() { _ = insp.Close() })
	return insp
}

// startServer запускает worker в фоне и останавливает его в конце теста.
//
// Start, а не Run: Run блокируется до SIGTERM/SIGINT и сам перехватывает
// сигналы, а Start сразу возвращает ошибку, если сервер не смог стартовать.
//
// Незаданные поля Config заполняются значениями, удобными для тестов:
//   - LogLevel = FatalLevel — внутренние логи Asynq не мешают читать вывод;
//   - TaskCheckInterval = 50ms — по умолчанию worker, у которого кончились
//     задачи, засыпает на 0.5–1.5 с, и каждый шаг теста ждал бы его;
//   - DelayedTaskCheckInterval = 50ms — по умолчанию scheduled- и retry-задачи
//     переносятся в pending раз в 5 секунд.
func startServer(t *testing.T, opt asynq.RedisClientOpt, cfg asynq.Config, h asynq.Handler) *asynq.Server {
	t.Helper()
	if cfg.LogLevel == 0 {
		cfg.LogLevel = asynq.FatalLevel
	}
	if cfg.TaskCheckInterval == 0 {
		cfg.TaskCheckInterval = 50 * time.Millisecond
	}
	if cfg.DelayedTaskCheckInterval == 0 {
		cfg.DelayedTaskCheckInterval = 50 * time.Millisecond
	}
	srv := asynq.NewServer(opt, cfg)
	require.NoError(t, srv.Start(h))
	t.Cleanup(srv.Shutdown)
	return srv
}

// noDelay — RetryDelayFunc без задержки: retry-задача вернётся в pending
// на ближайшем проходе forwarder'а.
func noDelay(int, error, *asynq.Task) time.Duration { return 0 }

// waitState ждёт, пока задача перейдёт в состояние want, и возвращает её TaskInfo.
func waitState(t *testing.T, insp *asynq.Inspector, queue, id string, want asynq.TaskState) *asynq.TaskInfo {
	t.Helper()
	var last *asynq.TaskInfo
	require.Eventually(t, func() bool {
		info, err := insp.GetTaskInfo(queue, id)
		if err != nil {
			return false
		}
		last = info
		return info.State == want
	}, waitTimeout, pollTick, "задача %s не дошла до состояния %s", id, want)
	return last
}

// waitDeleted ждёт, пока задача исчезнет из Redis (успех без Retention или RevokeTask).
func waitDeleted(t *testing.T, insp *asynq.Inspector, queue, id string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := insp.GetTaskInfo(queue, id)
		return errors.Is(err, asynq.ErrTaskNotFound)
	}, waitTimeout, pollTick, "задача %s не удалилась из Redis", id)
}

// receive читает значение из канала, а не дождавшись — валит тест,
// вместо того чтобы зависнуть до глобального таймаута go test.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitTimeout):
		t.Fatalf("не дождались значения из канала за %v", waitTimeout)
		var zero T
		return zero
	}
}

// noop — handler, который ничего не делает.
func noop(context.Context, *asynq.Task) error { return nil }
