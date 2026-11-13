package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"github.com/syncname/go-asynq-redis/internal/tasks"
)

// ═══════════════════════════════════════════════════════════════════════════
// 5.1 Delayed tasks
// ═══════════════════════════════════════════════════════════════════════════

// TestSchedule_RelativeAndAbsolute проверяет, что ProcessIn и ProcessAt кладут
// задачу в scheduled, а NextProcessAt равен запрошенному моменту.
func TestSchedule_RelativeAndAbsolute(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	// Задача 1: относительное время (ProcessIn).
	taskRelative, err := tasks.NewEmailTask(tasks.EmailPayload{To: "relative@void.local"})
	require.NoError(t, err, "не удалось создать первую задачу")
	before := time.Now()
	infoRelative, err := client.Enqueue(taskRelative, asynq.ProcessIn(30*time.Minute))
	require.NoError(t, err, "ошибка постановки первой задачи в очередь")
	after := time.Now()

	// Задача 2: абсолютное время (ProcessAt). Момент обязательно в будущем:
	// ProcessAt в прошлом — не ошибка, задача просто сразу встанет в pending
	// (см. TestSchedule_ProcessAtInPast). Поэтому не хардкодим дату, а берём
	// завтрашние 09:00 UTC.
	targetDate := time.Now().UTC().Truncate(24 * time.Hour).Add(24*time.Hour + 9*time.Hour)
	taskAbsolute, err := tasks.NewEmailTask(tasks.EmailPayload{To: "absolute@void.local"})
	require.NoError(t, err, "не удалось создать вторую задачу")
	infoAbsolute, err := client.Enqueue(taskAbsolute, asynq.ProcessAt(targetDate))
	require.NoError(t, err, "ошибка постановки второй задачи в очередь")

	// Вспомогательная функция: проверяет State=Scheduled и выводит подробности.
	// Так в логе теста сразу видно, в какой именно момент задача проснётся.
	verifyAndLog := func(name string, enqueuedInfo *asynq.TaskInfo) *asynq.TaskInfo {
		got, err := insp.GetTaskInfo(enqueuedInfo.Queue, enqueuedInfo.ID)
		require.NoError(t, err, "[%s] ошибка получения информации о задаче", name)
		require.Equal(t, asynq.TaskStateScheduled, got.State, "[%s] задача должна ждать в scheduled", name)
		t.Logf("=== %s ===", name)
		t.Logf("  Task ID:           %s", got.ID)
		t.Logf("  Queue:             %s", got.Queue)
		t.Logf("  Type:              %s", got.Type)
		t.Logf("  State:             %s", got.State)
		t.Logf("  NextProcessAt:     %v", got.NextProcessAt.Format(time.RFC3339))
		t.Logf("  Max Retry:         %d", got.MaxRetry)
		return got
	}

	rel := verifyAndLog("Относительное расписание (ProcessIn 30m)", infoRelative)
	abs := verifyAndLog("Абсолютное расписание (ProcessAt завтра 09:00 UTC)", infoAbsolute)

	// Момент запуска Asynq хранит как Unix-время в секундах (score в ZSET),
	// поэтому для ProcessIn сравниваем с окном, а не на точное равенство.
	require.WithinRange(t, rel.NextProcessAt,
		before.Add(30*time.Minute).Truncate(time.Second), after.Add(30*time.Minute))
	require.True(t, abs.NextProcessAt.Equal(targetDate),
		"NextProcessAt = %s, ожидали %s", abs.NextProcessAt, targetDate)
}

// TestSchedule_ProcessAtInPast: ProcessAt с моментом в прошлом не возвращает
// ошибку — Asynq молча ставит задачу в pending, и она выполнится сразу.
// Легко нарваться, если дата в коде или в тесте захардкожена и «протухла».
func TestSchedule_ProcessAtInPast(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	task := asynq.NewTask("test:past", nil)
	info, err := client.Enqueue(task, asynq.ProcessAt(time.Now().Add(-time.Hour)))
	require.NoError(t, err, "прошедший момент — не ошибка")
	require.Equal(t, asynq.TaskStatePending, info.State)

	got, err := insp.GetTaskInfo(info.Queue, info.ID)
	require.NoError(t, err)
	t.Logf("ProcessAt(час назад) → state=%s", got.State)
	require.Equal(t, asynq.TaskStatePending, got.State)
}

// TestSchedule_SecondPrecision показывает, что «не раньше, чем» у отложенных
// задач верно только с точностью до секунды.
//
// Момент запуска хранится как processAt.Unix() (дробная часть отбрасывается),
// а forwarder переносит задачу в pending, когда score <= time.Now().Unix().
// Задача с ProcessAt = X.900 станет доступна уже в X.000 — почти на секунду
// раньше запрошенного. Если важна точность до миллисекунд, проверяйте время
// в самом handler'е.
func TestSchedule_SecondPrecision(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)

	started := make(chan time.Time, 1)
	startServer(t, opt, asynq.Config{Concurrency: 1},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			started <- time.Now()
			return nil
		}))

	// Граница секунды через 2 с, а сам момент — на 900 мс позже неё.
	secondBoundary := time.Now().Truncate(time.Second).Add(2 * time.Second)
	processAt := secondBoundary.Add(900 * time.Millisecond)
	_, err := client.Enqueue(asynq.NewTask("test:at", nil), asynq.ProcessAt(processAt))
	require.NoError(t, err)

	got := receive(t, started)
	t.Logf("Запрошено:        %s", processAt.Format("15:04:05.000"))
	t.Logf("Handler стартовал: %s (на %v раньше запрошенного)",
		got.Format("15:04:05.000"), processAt.Sub(got).Round(time.Millisecond))

	require.False(t, got.Before(secondBoundary), "раньше начала секунды задача стартовать не может")
	require.True(t, got.Before(processAt), "ожидали старт раньше запрошенного момента из-за секундной точности")
}

// ═══════════════════════════════════════════════════════════════════════════
// 5.2 Retries
// ═══════════════════════════════════════════════════════════════════════════

// TestRetry_NormalAndSkipRetry проверяет две ветки retry-логики Asynq.
//
//  1. Обычная ошибка из handler'а трактуется как временная: Asynq инкрементирует
//     Retried и ставит задачу обратно в retry с задержкой из RetryDelayFunc.
//     Когда Retried достигает MaxRetry — задача уходит в archived.
//     Доказываем: при MaxRetry(2) handler вызывается ровно 3 раза (1 + 2 retry).
//
//  2. Ошибка, обёрнутая через asynq.SkipRetry, уходит в archived сразу, минуя
//     retry-цикл. MaxRetry игнорируется. Это для логических ошибок (битый
//     payload, нарушение бизнес-правил) — ретраить их бессмысленно.
//
// Как читать вывод:
//   - "[handler] test:..." печатает сам handler на каждом вызове;
//   - блок "=== ... ===" — финальная сводка из TaskInfo (то же, что в Asynqmon);
//   - require.EqualValues внизу — машинная проверка счётчиков.
func TestRetry_NormalAndSkipRetry(t *testing.T) {
	// miniredis — in-process Redis на Go, автоматически останавливается при
	// завершении теста через t.Cleanup. Для Asynq это полная замена настоящего.
	opt := newRedis(t)

	// atomic.Int32, потому что handler крутится в worker-горутине Asynq,
	// а читаем счётчик мы из тестовой — обычный int дал бы data race.
	var normalAttempts, skipAttempts atomic.Int32

	// asynq.HandlerFunc — adapter из функции в интерфейс asynq.Handler,
	// как http.HandlerFunc в стандартной библиотеке.
	handler := asynq.HandlerFunc(func(_ context.Context, task *asynq.Task) error {
		switch task.Type() {
		case "test:normal":
			n := normalAttempts.Add(1) // Add возвращает новое значение — удобно для лога
			t.Logf("  [handler] test:normal — попытка #%d, возвращаем обычную ошибку", n)
			return fmt.Errorf("transient failure on attempt %d", n)

		case "test:skip":
			n := skipAttempts.Add(1)
			t.Logf("  [handler] test:skip — попытка #%d, возвращаем SkipRetry", n)
			// ⚠ Именно %w, не %v/%s. Иначе errors.Is(err, asynq.SkipRetry) вернёт
			// false, и Asynq ретрайнет как обычно. Самая частая бага в handler'ах.
			return fmt.Errorf("permanent failure: %w", asynq.SkipRetry)
		}
		return nil
	})

	startServer(t, opt, asynq.Config{
		Concurrency: 1, // последовательная обработка — предсказуемые логи
		RetryDelayFunc: func(int, error, *asynq.Task) time.Duration {
			return 50 * time.Millisecond
		},
		// ВАЖНО для тестов с retry. По умолчанию forwarder проверяет retry-набор
		// раз в 5 секунд, и 50-миллисекундная задержка превращается в 5-секундную.
		// Опускаем интервал до уровня RetryDelay. (Второй такой же рычаг —
		// TaskCheckInterval, его выставляет startServer.)
		DelayedTaskCheckInterval: 50 * time.Millisecond,
	}, handler)

	client := newClient(t, opt)
	insp := newInspector(t, opt) // клиент для интроспекции состояния

	// MaxRetry(2) = 1 исходная + до 2 retry. У нас все 3 упадут → archived.
	normalTask := asynq.NewTask("test:normal", []byte(`{}`))
	normalInfo, err := client.Enqueue(normalTask, asynq.MaxRetry(2))
	require.NoError(t, err)

	// MaxRetry(5) специально завышен — наглядно показать, что SkipRetry его игнорирует.
	skipTask := asynq.NewTask("test:skip", []byte(`{}`))
	skipInfo, err := client.Enqueue(skipTask, asynq.MaxRetry(5))
	require.NoError(t, err)

	// waitState крутит require.Eventually: переходы pending → active → retry →
	// archived асинхронны. time.Sleep тоже работал бы, но был бы хрупким:
	// на медленной машине мало, на быстрой избыточно.
	//
	// counter передаём *указателем*, чтобы прочитать его уже ПОСЛЕ ожидания —
	// иначе зафиксируем snapshot до того, как retry успели произойти.
	waitArchived := func(name string, info *asynq.TaskInfo, counter *atomic.Int32, want int32) *asynq.TaskInfo {
		final := waitState(t, insp, info.Queue, info.ID, asynq.TaskStateArchived)

		attempts := counter.Load() // безопасно: задача уже в архиве, retry-цикл окончен
		t.Logf("=== %s ===", name)
		t.Logf("  Task ID:           %s", final.ID)
		t.Logf("  Type:              %s", final.Type)
		t.Logf("  State:             %s", final.State)
		t.Logf("  MaxRetry:          %d", final.MaxRetry)
		// Retried считает ПОВТОРЫ, не общее число попыток: для normal с 3 вызовами
		// это 2, для skip — 0 (SkipRetry уходит в archive минуя retry-логику).
		t.Logf("  Retried (счётчик): %d", final.Retried)
		t.Logf("  Фактически попыток у handler'а: %d (ожидалось %d)", attempts, want)
		if final.LastErr != "" {
			t.Logf("  LastErr:           %s", final.LastErr)
		}
		if !final.LastFailedAt.IsZero() {
			t.Logf("  LastFailedAt:      %s", final.LastFailedAt.Format(time.RFC3339Nano))
		}
		return final
	}

	normalFinal := waitArchived("Обычная ошибка с MaxRetry(2)", normalInfo, &normalAttempts, 3)
	// EqualValues, не Equal: counter.Load() это int32, а 3 — int, у Equal не сойдутся.
	require.EqualValues(t, 3, normalAttempts.Load(), "normal: должно быть 1+2=3 вызова")
	require.Equal(t, 2, normalFinal.Retried)
	require.Contains(t, normalFinal.LastErr, "attempt 3", "в LastErr — текст последней ошибки")

	skipFinal := waitArchived("SkipRetry с MaxRetry(5)", skipInfo, &skipAttempts, 1)
	require.EqualValues(t, 1, skipAttempts.Load(), "skip: SkipRetry прерывает цикл после первого вызова")
	require.Equal(t, 0, skipFinal.Retried)
	require.Contains(t, skipFinal.LastErr, "permanent failure")
}

// TestRetry_DelayFuncArguments проверяет, что получает RetryDelayFunc.
//
// n — сколько раз задача УЖЕ ретраилась: при первой ошибке n = 0. После
// последней попытки функция не вызывается вовсе — задача сразу уходит в
// archive. Для MaxRetry(3) это ровно n = 0, 1, 2. Вместе с n приходят ошибка
// и сама задача — по ним можно выбирать задержку (раздел 6.2).
func TestRetry_DelayFuncArguments(t *testing.T) {
	opt := newRedis(t)

	var (
		mu    sync.Mutex
		calls []string
	)
	var attempts atomic.Int32
	startServer(t, opt, asynq.Config{
		Concurrency: 1,
		RetryDelayFunc: func(n int, err error, task *asynq.Task) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, fmt.Sprintf("n=%d type=%s err=%v", n, task.Type(), err))
			return 0
		},
	}, asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return fmt.Errorf("attempt %d failed", attempts.Add(1))
	}))

	client := newClient(t, opt)
	insp := newInspector(t, opt)
	info, err := client.Enqueue(asynq.NewTask("test:flaky", nil), asynq.MaxRetry(3))
	require.NoError(t, err)

	final := waitState(t, insp, info.Queue, info.ID, asynq.TaskStateArchived)

	mu.Lock()
	defer mu.Unlock()
	for _, c := range calls {
		t.Logf("  RetryDelayFunc(%s)", c)
	}
	require.Equal(t, []string{
		"n=0 type=test:flaky err=attempt 1 failed",
		"n=1 type=test:flaky err=attempt 2 failed",
		"n=2 type=test:flaky err=attempt 3 failed",
	}, calls, "после 4-й (последней) попытки RetryDelayFunc не вызывается")
	require.EqualValues(t, 4, attempts.Load())
	require.Equal(t, 3, final.Retried)
}

// ═══════════════════════════════════════════════════════════════════════════
// 5.3 Timeout и Deadline
// ═══════════════════════════════════════════════════════════════════════════

// TestTimeout_Cancellation проверяет, что Asynq отменяет ctx handler'а
// при истечении Timeout/Deadline.
//
//  1. Timeout(d) — относительный лимит на ОДНУ попытку. ctx отменится через d
//     от момента, когда worker взял задачу.
//  2. Deadline(t) — абсолютный момент. ctx отменится в момент t, независимо от
//     того, когда именно стартовал handler.
//
// ⚠ КРИТИЧНО: Asynq хранит Timeout и Deadline с СЕКУНДНОЙ точностью.
// В client.go видно:
//
//	Timeout:  int64(timeout.Seconds())  // sub-second значения → 0
//	Deadline: deadline.Unix()           // миллисекунды теряются
//
// Что из этого следует для тестов:
//   - Timeout(500*time.Millisecond) превращается в 0 → Asynq трактует это как
//     "таймаута нет" и применяет дефолт 30 минут. Handler ждёт ctx.Done() 30
//     минут, тест падает по таймауту. Минимум, который работает, — 1 секунда.
//   - Deadline(now + 100*time.Millisecond) после .Unix()-округления может
//     оказаться в прошлом к моменту, когда worker подберёт задачу. Processor
//     перед вызовом handler'а проверяет:
//     select { case <-ctx.Done(): handleFailedMessage(...) default: }
//     и если ctx уже отменён — попытка засчитывается БЕЗ вызова handler'а
//     (см. TestTimeout_ExpiredDeadlineBurnsRetries). Запас 2+ секунды страхует от этого.
//
// Поэтому в тесте используем целые секунды (Timeout=1s, Deadline=now+2s).
// Если в продакшене нужна миллисекундная точность — оборачивайте свой ctx
// руками: `ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)`.
//
// Главное правило для handler'а в проде: уважать ctx. Что бывает, если его
// игнорировать, — в TestTimeout_HandlerIgnoringContext.
//
// Как читать вывод:
//   - Логи показывают: когда handler стартовал, когда увидел отмену, сколько прошло;
//   - ctx.Deadline() — Asynq прокидывает дедлайн в контекст, handler может его прочитать;
//   - для Deadline видно, что дедлайн в ctx — ровная секунда, без миллисекунд.
func TestTimeout_Cancellation(t *testing.T) {
	opt := newRedis(t)

	// Канал для передачи наблюдений из handler'а наружу. Канал заменяет здесь
	// atomic/mutex — каждое наблюдение приходит ровно один раз.
	type observation struct {
		name      string
		startedAt time.Time
		cancelled time.Time
		deadline  time.Time
		err       error
	}
	obs := make(chan observation, 2)

	handler := asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		o := observation{name: task.Type(), startedAt: time.Now()}
		// ctx.Deadline() возвращает (deadline, ok). У задач Asynq всегда есть
		// дедлайн — даже без явного Timeout стоит дефолт 30 минут.
		o.deadline, _ = ctx.Deadline()
		// Просто ждём, пока ctx отменят. В реальном коде здесь была бы полезная
		// работа с регулярной проверкой <-ctx.Done() в select.
		<-ctx.Done()
		o.cancelled = time.Now()
		o.err = ctx.Err()
		obs <- o
		return ctx.Err()
	})

	startServer(t, opt, asynq.Config{
		Concurrency: 2, // обе задачи параллельно — нагляднее
	}, handler)

	client := newClient(t, opt)

	// Timeout(1*time.Second) — минимум, который Asynq не округляет в 0.
	// MaxRetry(0), чтобы после отмены задача сразу ушла в archived, а не в retry.
	// Иначе пришло бы лишнее наблюдение и тест растянулся бы на retry-delay.
	timeoutTask := asynq.NewTask("test:timeout", []byte(`{}`))
	_, err := client.Enqueue(timeoutTask, asynq.Timeout(1*time.Second), asynq.MaxRetry(0))
	require.NoError(t, err)

	// Deadline(now + 2*time.Second) — запас 2 секунды, чтобы .Unix()-округление
	// не положило дедлайн в прошлое.
	deadlineAt := time.Now().Add(2 * time.Second)
	deadlineTask := asynq.NewTask("test:deadline", []byte(`{}`))
	_, err = client.Enqueue(deadlineTask, asynq.Deadline(deadlineAt), asynq.MaxRetry(0))
	require.NoError(t, err)

	// Собираем оба наблюдения — порядок неважен, обе задачи параллельны.
	results := map[string]observation{}
	for i := 0; i < 2; i++ {
		o := receive(t, obs)
		results[o.name] = o
	}

	for _, name := range []string{"test:timeout", "test:deadline"} {
		o := results[name]
		t.Logf("=== %s ===", name)
		t.Logf("  Handler стартовал:     %s", o.startedAt.Format("15:04:05.000"))
		t.Logf("  Handler увидел отмену: %s", o.cancelled.Format("15:04:05.000"))
		t.Logf("  Прошло до отмены:      %v", o.cancelled.Sub(o.startedAt))
		t.Logf("  ctx.Deadline():        %s", o.deadline.Format("15:04:05.000"))
		t.Logf("  ctx.Err():             %v", o.err)
		require.ErrorIs(t, o.err, context.DeadlineExceeded, "[%s] ctx должен был быть отменён по дедлайну", name)
	}

	// Timeout отсчитывается от момента, когда worker взял задачу, с обычной
	// точностью часов — округляется только само значение Timeout. Окно ±500мс —
	// запас на медленный CI.
	timeoutElapsed := results["test:timeout"].cancelled.Sub(results["test:timeout"].startedAt)
	require.InDelta(t, 1*time.Second, timeoutElapsed, float64(500*time.Millisecond),
		"test:timeout: отмена через %v, ожидали ~1с", timeoutElapsed)

	// Deadline абсолютный: момент отмены не зависит от того, когда worker
	// подобрал задачу, — это ровная секунда deadlineAt.Unix().
	wantDeadline := time.Unix(deadlineAt.Unix(), 0)
	dl := results["test:deadline"]
	require.True(t, dl.deadline.Equal(wantDeadline),
		"ctx.Deadline() = %s, ожидали %s (без миллисекунд)", dl.deadline, wantDeadline)
	require.WithinDuration(t, wantDeadline, dl.cancelled, 500*time.Millisecond)
}

// TestTimeout_SecondPrecision доказывает архитектурную особенность Asynq:
// потерю миллисекундной точности при сериализации параметров времени.
//
// ЧТО МЫ ДОКАЗЫВАЕМ:
//
//  1. Timeout(500*time.Millisecond):
//     При сериализации Asynq делает int64(0.5). Дробная часть отбрасывается = 0.
//     В исходниках воркера Asynq (processor.go, computeDeadline) есть логика:
//     if msg.Timeout == 0 && msg.Deadline == 0 { return now.Add(defaultTimeout) }
//     В тесте мы наглядно видим, что задаче выделено 30 минут.
//
//  2. Deadline(now + 500*time.Millisecond):
//     Сериализуется через t.Unix(), что оставляет только целые секунды.
//     Когда Asynq восстанавливает время, наносекунды равны нулю.
//
// ВАЖНО ПРО ТЕСТИРОВАНИЕ:
// Мы не используем require.* внутри HandlerFunc.
// Если require падает внутри горутины воркера, вызывается t.FailNow(),
// который молча убивает горутину (runtime.Goexit()), а основной поток теста
// ждёт канал. Все проверки делаем в тестовой горутине, а каналы читаем
// через receive — с таймаутом, а не вечным <-ch.
func TestTimeout_SecondPrecision(t *testing.T) {
	opt := newRedis(t)

	// Структура для безопасной передачи контекста из воркера в основной поток теста.
	type handlerResult struct {
		hasDeadline bool
		deadline    time.Time
	}

	timeoutCh := make(chan handlerResult, 1)
	deadlineCh := make(chan handlerResult, 1)

	// Хендлер максимально "глупый": только извлекает данные и отдает в канал.
	handler := asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		dl, ok := ctx.Deadline()
		res := handlerResult{
			hasDeadline: ok,
			deadline:    dl,
		}

		switch task.Type() {
		case "test:timeout":
			timeoutCh <- res
		case "test:deadline":
			deadlineCh <- res
		}
		return nil
	})

	startServer(t, opt, asynq.Config{Concurrency: 1}, handler)
	client := newClient(t, opt)

	// =========================================================================
	// КЕЙС 1: Timeout < 1 секунды
	// =========================================================================

	t.Logf("=== Запуск проверки Timeout ===")
	_, err := client.Enqueue(
		asynq.NewTask("test:timeout", []byte(`{}`)),
		asynq.Timeout(500*time.Millisecond), // <-- 0.5с станет 0
	)
	require.NoError(t, err)

	timeoutResult := receive(t, timeoutCh)
	require.True(t, timeoutResult.hasDeadline, "Asynq должен был проставить дедлайн")

	// КАК МЫ "ВИДИМ" 30 МИНУТ:
	// У интерфейса context.Context нет метода Timeout(). Мы узнаем
	// выделенное время, вычитая текущее время из абсолютного дедлайна.
	rawTimeLeft := time.Until(timeoutResult.deadline)
	roundedTimeLeft := rawTimeLeft.Round(time.Second)

	t.Logf("  'Сырое' оставшееся время: %v", rawTimeLeft)
	t.Logf("  Оно же, округленное: %v", roundedTimeLeft)

	require.Equal(t, 30*time.Minute, roundedTimeLeft,
		"Таймаут 500ms превратился в 0, и воркер применил дефолтные 30 минут")
	t.Logf("  [УСПЕХ] Задаче выделено 30 минут вместо 500ms")

	// =========================================================================
	// КЕЙС 2: Потеря точности Deadline
	// =========================================================================

	t.Logf("=== Запуск проверки Deadline ===")
	now := time.Now()
	// Формируем дедлайн с гарантированной дробной частью (наносекундами).
	// Добавляем 5 секунд, чтобы дедлайн гарантированно был в будущем.
	expectedDeadline := now.Add(5*time.Second + 500*time.Millisecond)

	_, err = client.Enqueue(
		asynq.NewTask("test:deadline", []byte(`{}`)),
		asynq.Deadline(expectedDeadline), // <-- t.Unix() отбросит дробную часть
	)
	require.NoError(t, err)

	deadlineResult := receive(t, deadlineCh)
	require.True(t, deadlineResult.hasDeadline, "Asynq должен был проставить дедлайн")

	actualDeadline := deadlineResult.deadline

	// ПОЧЕМУ ВРЕМЯ БЕЗ ДРОБИ:
	// Специфика time.RFC3339Nano: если наносекунды = 0, точка не печатается.
	formatted := actualDeadline.Format(time.RFC3339Nano)

	t.Logf("  Отправлено (наносекунды): %d", expectedDeadline.Nanosecond())
	t.Logf("  Получено   (наносекунды): %d", actualDeadline.Nanosecond())
	t.Logf("  Время в контексте (строка): %s", formatted)

	// Проверяем, что точка исчезла и наносекунды обнулились
	require.False(t, strings.Contains(formatted, "."),
		"Дробная часть должна отсутствовать (time.RFC3339Nano скрывает нули)")
	require.Equal(t, 0, actualDeadline.Nanosecond(),
		"Дробная часть времени должна быть равна 0")

	t.Logf("  Потерянная точность составила: %v", expectedDeadline.Sub(actualDeadline))
	t.Logf("  [УСПЕХ] Миллисекунды потерялись при сериализации Deadline")
}

// TestTimeout_HandlerIgnoringContext показывает, что бывает с handler'ом,
// который не смотрит на ctx.
//
// Asynq его не ждёт: по истечении Timeout processor сам засчитывает попытку
// как проваленную (retry или archive) и освобождает слот Concurrency.
// Остановить горутину снаружи в Go нельзя, поэтому handler продолжает работать.
// Следующая попытка той же задачи стартует, пока предыдущая ещё жива: две
// копии одной задачи выполняются параллельно, хотя Concurrency = 1.
// А результат, который в итоге вернёт handler, уже никого не интересует.
func TestTimeout_HandlerIgnoringContext(t *testing.T) {
	opt := newRedis(t)

	const work = 2500 * time.Millisecond // дольше Timeout(1s)
	var attempts, running, maxRunning atomic.Int32
	finished := make(chan struct{}, 2)

	handler := asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
		defer func() { finished <- struct{}{} }()
		attempts.Add(1)
		n := running.Add(1)
		for m := maxRunning.Load(); n > m && !maxRunning.CompareAndSwap(m, n); m = maxRunning.Load() {
		}
		time.Sleep(work) // «тяжёлая работа», которая не проверяет ctx
		running.Add(-1)
		return nil
	})

	startServer(t, opt, asynq.Config{
		Concurrency:    1,
		RetryDelayFunc: noDelay,
	}, handler)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	info, err := client.Enqueue(asynq.NewTask("test:stubborn", nil),
		asynq.Timeout(time.Second), asynq.MaxRetry(1))
	require.NoError(t, err)

	// Ждём, пока обе копии доработают: Shutdown их не дождётся, и иначе
	// «зомби»-горутина пережила бы тест.
	receive(t, finished)
	receive(t, finished)

	got := waitState(t, insp, info.Queue, info.ID, asynq.TaskStateArchived)
	t.Logf("Попыток: %d, одновременно работало: %d (Concurrency = 1)", attempts.Load(), maxRunning.Load())
	t.Logf("Итог: state=%s retried=%d last_err=%q", got.State, got.Retried, got.LastErr)

	require.EqualValues(t, 2, attempts.Load())
	require.EqualValues(t, 2, maxRunning.Load(), "вторая попытка должна стартовать, пока первая ещё работает")
	require.Contains(t, got.LastErr, "deadline exceeded", "обе попытки засчитаны как таймаут, хотя handler вернул nil")
}

// TestTimeout_ExpiredDeadlineBurnsRetries: если Deadline уже наступил, handler
// не вызывается ни разу — processor проверяет ctx до его запуска. Но задача
// не выбрасывается: она проходит все MaxRetry попыток с ошибкой
// "context deadline exceeded" и оседает в архиве среди настоящих ошибок.
// Поэтому Deadline плохо подходит на роль «срока годности» (раздел 6.2).
func TestTimeout_ExpiredDeadlineBurnsRetries(t *testing.T) {
	opt := newRedis(t)

	var calls atomic.Int32
	startServer(t, opt, asynq.Config{Concurrency: 1, RetryDelayFunc: noDelay},
		asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
			calls.Add(1)
			return nil
		}))
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	info, err := client.Enqueue(asynq.NewTask("test:expired", nil),
		asynq.Deadline(time.Now().Add(-time.Minute)), asynq.MaxRetry(3))
	require.NoError(t, err, "дедлайн в прошлом — не ошибка Enqueue")

	got := waitState(t, insp, info.Queue, info.ID, asynq.TaskStateArchived)
	t.Logf("Вызовов handler'а: %d; state=%s retried=%d last_err=%q",
		calls.Load(), got.State, got.Retried, got.LastErr)

	require.Zero(t, calls.Load(), "handler не должен вызываться после дедлайна")
	require.Equal(t, 3, got.Retried, "все ретраи потрачены впустую")
	require.Contains(t, got.LastErr, "deadline exceeded")
}

// ═══════════════════════════════════════════════════════════════════════════
// 5.4 Маршрутизация: типы и очереди
// ═══════════════════════════════════════════════════════════════════════════

// TestRouting_QueueOption проверяет, что опция Queue("...") роутит задачу
// в указанную очередь, а без неё задача попадает в "default".
//
// Маршрутизация в Asynq двухуровневая:
//   - Уровень 1 (тип → handler) — через ServeMux (см. TestRouting_ServeMuxMatching);
//   - Уровень 2 (задача → очередь) — через опцию Queue("name") при Enqueue.
//
// Здесь проверяем именно уровень 2: куда физически легла задача в Redis.
// Server здесь не запускаем намеренно: с ним Pending мгновенно ушёл бы в 0,
// а нам как раз и нужно увидеть, в какой очереди задача "лежит".
//
// Как читать вывод: лог говорит, какая задача в какую очередь ушла; сводка
// "Queue X" показывает Pending — сколько задач физически в очереди.
func TestRouting_QueueOption(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	// С опцией Queue → в указанную очередь.
	taskCritical, err := tasks.NewEmailTask(tasks.EmailPayload{To: "ceo@void.local"})
	require.NoError(t, err)
	critInfo, err := client.Enqueue(taskCritical, asynq.Queue("critical"))
	require.NoError(t, err)
	require.Equal(t, "critical", critInfo.Queue)

	// Без опции Queue → в "default" по умолчанию.
	taskDefault, err := tasks.NewEmailTask(tasks.EmailPayload{To: "user@void.local"})
	require.NoError(t, err)
	defInfo, err := client.Enqueue(taskDefault)
	require.NoError(t, err)
	require.Equal(t, "default", defInfo.Queue)

	t.Logf("Enqueue: critical=%s, default=%s", critInfo.ID, defInfo.ID)

	// Inspector.Queues() возвращает имена всех очередей, где когда-либо что-то лежало.
	queues, err := insp.Queues()
	require.NoError(t, err)
	t.Logf("Все очереди в Redis: %v", queues)

	// Проверяем, что в каждой ровно одна Pending-задача.
	for _, qname := range []string{"critical", "default"} {
		q, err := insp.GetQueueInfo(qname)
		require.NoError(t, err)
		t.Logf("Queue %q: Size=%d, Pending=%d, Active=%d",
			qname, q.Size, q.Pending, q.Active)
		require.Equal(t, 1, q.Pending, "в очереди %q должна быть ровно 1 задача", qname)
	}
}

// TestRouting_ServeMuxMatching проверяет правила выбора handler'а в ServeMux.
// Сервер не нужен: mux.Handler(task) возвращает handler и сработавший паттерн.
//
//   - точное совпадение сильнее префиксного;
//   - среди префиксов побеждает самый длинный;
//   - в отличие от http.ServeMux, ЛЮБОЙ паттерн — префикс, а не только
//     оканчивающийся на ":". Handler для "email:send" получит и
//     "email:send:bulk", и даже "email:sender";
//   - для неизвестного типа — NotFoundHandler с ошибкой ErrHandlerNotFound.
func TestRouting_ServeMuxMatching(t *testing.T) {
	mux := asynq.NewServeMux()
	for _, pattern := range []string{"email:", "email:send", "webhook:", "webhook:partner:"} {
		mux.HandleFunc(pattern, noop)
	}

	cases := []struct {
		taskType string
		want     string
	}{
		{"email:send", "email:send"},                 // точное совпадение
		{"email:digest", "email:"},                   // префикс
		{"webhook:partner:acme", "webhook:partner:"}, // самый длинный префикс
		{"email:send:bulk", "email:send"},            // паттерн без ":" на конце — тоже префикс
		{"email:sender", "email:send"},               // …и даже без разделителя
		{"image:resize", ""},                         // ничего не подошло
	}
	for _, c := range cases {
		_, pattern := mux.Handler(asynq.NewTask(c.taskType, nil))
		t.Logf("%-22s → %q", c.taskType, pattern)
		require.Equal(t, c.want, pattern, "тип %q", c.taskType)
	}

	h, _ := mux.Handler(asynq.NewTask("image:resize", nil))
	err := h.ProcessTask(context.Background(), asynq.NewTask("image:resize", nil))
	require.ErrorIs(t, err, asynq.ErrHandlerNotFound)
	t.Logf("Неизвестный тип: %v", err)
}

// TestRouting_UnknownTypeGoesToRetry: задача, тип которой не зарегистрирован в
// mux, не теряется и не уходит в архив сразу — она падает с ErrHandlerNotFound
// и едет на ретраи, как обычная временная ошибка (раздел 7.3).
func TestRouting_UnknownTypeGoesToRetry(t *testing.T) {
	opt := newRedis(t)

	mux := asynq.NewServeMux()
	mux.Handle(tasks.TypeEmailSend, &tasks.EmailHandler{})
	startServer(t, opt, asynq.Config{Concurrency: 1}, mux)

	client := newClient(t, opt)
	insp := newInspector(t, opt)
	info, err := client.Enqueue(asynq.NewTask("image:resize", nil))
	require.NoError(t, err)

	got := waitState(t, insp, info.Queue, info.ID, asynq.TaskStateRetry)
	t.Logf("state=%s retried=%d/%d last_err=%q next=%s",
		got.State, got.Retried, got.MaxRetry, got.LastErr, got.NextProcessAt.Format(time.RFC3339))
	require.Equal(t, 1, got.Retried)
	require.Contains(t, got.LastErr, "handler not found")
}

// TestRouting_UnlistedQueueIsIgnored: worker обслуживает только очереди из
// Config.Queues. Если задать Queues и забыть "default", задачи без опции
// Queue будут копиться в pending, хотя worker запущен и свободен.
func TestRouting_UnlistedQueueIsIgnored(t *testing.T) {
	opt := newRedis(t)

	processed := make(chan string, 2)
	startServer(t, opt, asynq.Config{
		Concurrency: 1,
		Queues:      map[string]int{"critical": 1}, // "default" здесь нет
	}, asynq.HandlerFunc(func(ctx context.Context, _ *asynq.Task) error {
		q, _ := asynq.GetQueueName(ctx)
		processed <- q
		return nil
	}))

	client := newClient(t, opt)
	insp := newInspector(t, opt)

	defInfo, err := client.Enqueue(asynq.NewTask("test:job", []byte("default")))
	require.NoError(t, err)
	_, err = client.Enqueue(asynq.NewTask("test:job", []byte("critical")), asynq.Queue("critical"))
	require.NoError(t, err)

	require.Equal(t, "critical", receive(t, processed))

	// Worker свободен и опрашивает очереди каждые ~50 мс — за полсекунды он
	// забрал бы задачу из default, если бы обслуживал её.
	require.Never(t, func() bool {
		info, err := insp.GetTaskInfo(defInfo.Queue, defInfo.ID)
		return err != nil || info.State != asynq.TaskStatePending
	}, 500*time.Millisecond, 50*time.Millisecond, "задача из default не должна обрабатываться")
	t.Logf("Задача в default так и осталась pending: очереди нет в Config.Queues")
}

// TestRouting_PriorityWeights демонстрирует, что веса очередей в Config.Queues
// реально влияют на порядок выборки задач worker'ом — что Asynq не просто
// хранит вес как метаданные, а действительно использует его при каждом pickup'е.
//
// Идея: кладём по 100 задач в три очереди с весами 6:3:1, запускаем одного
// worker'а (Concurrency=1, чтобы порядок не смешивался) и логируем накопленное
// распределение по очередям каждые 50 обработанных задач.
//
// Как устроена «лотерея»: перед каждой выборкой Asynq строит список, где имя
// очереди повторяется столько раз, каков её вес, перемешивает его и идёт по
// очередям в получившемся порядке до первой непустой. Пока все три очереди
// не пусты, critical выбирается с вероятностью 6/10, default — 3/10, low — 1/10.
//
// Что должно быть видно в логах (числа примерные — алгоритм вероятностный):
//   - После  50 обработанных: ~30 critical, ~15 default, ~5  low
//   - После 100:               ~60        ~30          ~10
//   - После 150:               ~90        ~45          ~15
//   - Дальше critical исчерпан (cap=100), и пропорция между default и low
//     становится 3:1.
//   - После 300: все три очереди обработаны полностью (по 100).
//
// Это статистический тест: точные числа в каждом запуске будут разные.
// Поэтому строгая проверка одна и с огромным запасом: после 100 задач
// critical обработано больше, чем low (60 против 10 в среднем, вероятность
// ложного провала — порядка 1e-13). Остальное — логи для глазной инспекции.
func TestRouting_PriorityWeights(t *testing.T) {
	const tasksPerQueue = 100
	const checkpointEvery = 50

	opt := newRedis(t)
	client := newClient(t, opt)

	// Сначала наполняем очереди и только потом запускаем worker. Если стартовать
	// worker раньше, он начнёт разбирать critical, пока default и low ещё
	// пусты, и распределение будет отражать порядок Enqueue, а не веса.
	for _, qname := range []string{"critical", "default", "low"} {
		for i := 0; i < tasksPerQueue; i++ {
			task := asynq.NewTask("test:priority", []byte(`{}`))
			_, err := client.Enqueue(task, asynq.Queue(qname))
			require.NoError(t, err)
		}
	}
	t.Logf("Положили по %d задач в 3 очереди (critical:default:low = 6:3:1), всего %d",
		tasksPerQueue, 3*tasksPerQueue)

	// Счётчики по каждой очереди. Concurrency=1, но атомики берём идиоматично —
	// следующая итерация теста может захотеть увеличить параллельность.
	counts := map[string]*atomic.Int32{
		"critical": {},
		"default":  {},
		"low":      {},
	}
	var totalProcessed atomic.Int32
	snapshotAt100 := make(chan map[string]int32, 1)
	done := make(chan struct{})

	handler := asynq.HandlerFunc(func(ctx context.Context, _ *asynq.Task) error {
		// Asynq прокидывает имя очереди в ctx — это позволяет одному handler'у
		// знать, из какой очереди пришла задача, без отдельного payload-поля.
		qname, _ := asynq.GetQueueName(ctx)
		counts[qname].Add(1)

		n := totalProcessed.Add(1)
		snapshot := map[string]int32{
			"critical": counts["critical"].Load(),
			"default":  counts["default"].Load(),
			"low":      counts["low"].Load(),
		}
		// Логируем распределение на каждом checkpoint'е — это и есть основной
		// выхлоп теста, из которого видно работу приоритетов.
		if int(n)%checkpointEvery == 0 {
			t.Logf("После %3d задач: critical=%3d, default=%3d, low=%3d",
				n, snapshot["critical"], snapshot["default"], snapshot["low"])
		}
		if n == 100 {
			snapshotAt100 <- snapshot
		}
		if int(n) == 3*tasksPerQueue {
			close(done)
		}
		return nil
	})

	startServer(t, opt, asynq.Config{
		Concurrency: 1, // последовательность важна — параллельные worker'ы смешали бы порядок
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	}, handler)

	first100 := receive(t, snapshotAt100)
	require.Greater(t, first100["critical"], first100["low"],
		"после 100 задач critical должно быть обработано больше, чем low: %v", first100)

	receive(t, done)

	// Финальная проверка: каждая очередь обработана ровно tasksPerQueue раз.
	for q, c := range counts {
		require.EqualValues(t, tasksPerQueue, c.Load(),
			"очередь %s должна была обработать ровно %d задач", q, tasksPerQueue)
	}
}

// TestRouting_StrictPriority: с StrictPriority: true worker не трогает low,
// пока в critical есть хоть одна задача. Порядок постановки значения не имеет —
// low кладём первыми, а выполнятся они последними.
func TestRouting_StrictPriority(t *testing.T) {
	const perQueue = 10

	opt := newRedis(t)
	client := newClient(t, opt)
	for _, qname := range []string{"low", "critical"} {
		for i := 0; i < perQueue; i++ {
			_, err := client.Enqueue(asynq.NewTask("test:strict", nil), asynq.Queue(qname))
			require.NoError(t, err)
		}
	}

	var (
		mu    sync.Mutex
		order []string
	)
	done := make(chan struct{})
	startServer(t, opt, asynq.Config{
		Concurrency:    1,
		StrictPriority: true,
		Queues:         map[string]int{"critical": 6, "low": 1},
	}, asynq.HandlerFunc(func(ctx context.Context, _ *asynq.Task) error {
		q, _ := asynq.GetQueueName(ctx)
		mu.Lock()
		defer mu.Unlock()
		order = append(order, q)
		if len(order) == 2*perQueue {
			close(done)
		}
		return nil
	}))

	receive(t, done)
	mu.Lock()
	defer mu.Unlock()
	t.Logf("Порядок обработки: %v", order)
	for i, q := range order {
		want := "critical"
		if i >= perQueue {
			want = "low"
		}
		require.Equal(t, want, q, "задача #%d", i+1)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// 5.5 Unique tasks
// ═══════════════════════════════════════════════════════════════════════════

// TestUnique_TaskIDAndUnique проверяет оба механизма защиты от дубликатов.
//
//  1. TaskID(id) — явный ключ. Если задача с таким ID есть в очереди в любом
//     состоянии (pending, active, scheduled, retry, archived), Enqueue вернёт
//     ErrTaskIDConflict. Дедупликация только по КЛЮЧУ — payload не учитывается.
//
//  2. Unique(ttl) — дедупликация по хешу (type + payload + queue). Если в течение
//     ttl приходит задача с теми же байтами — Enqueue вернёт ErrDuplicateTask.
//     Меняется хоть один байт в payload — это уже другая задача для Asynq.
//
// Когда что брать:
//   - Есть бизнес-ключ ("отчёт user_id=42 за 2026-01-15") → TaskID, предсказуемее.
//   - Нет ключа, нужно отбить дважды кликнутую кнопку → Unique.
//
// Как читать вывод: каждый блок проверяет конкретную семантику, лог говорит,
// какую именно ошибку получили (или OK, если конфликта быть не должно).
func TestUnique_TaskIDAndUnique(t *testing.T) {
	opt := newRedis(t)
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	// ── Часть 1: TaskID ─────────────────────────────────────────────────────
	// Показываем, что TaskID конфликтует ДАЖЕ при разном payload —
	// дедупликация только по ключу, тело задачи не сравнивается.
	t.Log("=== TaskID: явный ключ, payload не учитывается ===")
	customID := "email:user:42:welcome"

	taskA, err := tasks.NewEmailTask(tasks.EmailPayload{UserID: 42, To: "alice@void.local"})
	require.NoError(t, err)
	_, err = client.Enqueue(taskA, asynq.TaskID(customID))
	require.NoError(t, err)
	t.Logf("  Первый Enqueue с TaskID=%q — OK", customID)

	// Тот же ID, но другой payload — всё равно конфликт.
	taskB, err := tasks.NewEmailTask(tasks.EmailPayload{UserID: 42, To: "alice@example.com"})
	require.NoError(t, err)
	_, err = client.Enqueue(taskB, asynq.TaskID(customID))
	// errors.Is, а не ==, потому что Asynq оборачивает ошибку через fmt.Errorf("%w").
	require.ErrorIs(t, err, asynq.ErrTaskIDConflict)
	t.Logf("  Второй Enqueue с тем же ID и другим payload — ErrTaskIDConflict ✓")

	// Подтверждаем: в Redis осталась ПЕРВАЯ задача (с alice@void.local).
	// Это важная деталь: при конфликте TaskID не перезаписывает существующую.
	got, err := insp.GetTaskInfo("default", customID)
	require.NoError(t, err)
	require.Contains(t, string(got.Payload), "alice@void.local",
		"при конфликте должна остаться первая задача, не вторая")
	t.Logf("  В Redis осталась первая задача (payload содержит alice@void.local) ✓")

	// Тот же ID в другой очереди — не конфликт: ключ задачи в Redis
	// строится как asynq:{<queue>}:t:<id>.
	_, err = client.Enqueue(taskB, asynq.TaskID(customID), asynq.Queue("critical"))
	require.NoError(t, err)
	t.Logf("  Тот же ID, но в critical — OK (TaskID scoped к очереди) ✓")

	// ── Часть 2: Unique по payload ──────────────────────────────────────────
	t.Log("=== Unique: дедупликация по хешу (type + payload + queue) ===")
	payload := tasks.EmailPayload{UserID: 7, To: "bob@void.local", Subject: "Hello"}
	uniqWindow := 10 * time.Minute

	task1, err := tasks.NewEmailTask(payload)
	require.NoError(t, err)
	_, err = client.Enqueue(task1, asynq.Unique(uniqWindow))
	require.NoError(t, err)
	t.Logf("  Первая задача с Unique — OK")

	// Тот же payload (байт-в-байт) → дубль.
	task2, err := tasks.NewEmailTask(payload)
	require.NoError(t, err)
	_, err = client.Enqueue(task2, asynq.Unique(uniqWindow))
	require.ErrorIs(t, err, asynq.ErrDuplicateTask)
	t.Logf("  Вторая задача с тем же payload — ErrDuplicateTask ✓")

	// Меняем один байт → уже другая задача для Asynq.
	pAlt := payload
	pAlt.Subject = "Hello!" // плюс восклицательный знак
	task3, err := tasks.NewEmailTask(pAlt)
	require.NoError(t, err)
	_, err = client.Enqueue(task3, asynq.Unique(uniqWindow))
	require.NoError(t, err)
	t.Logf("  Третья задача с изменённым Subject — OK (изменение payload снимает дедупликацию) ✓")

	// Тот же payload, но в другую очередь → не дубль. Unique привязан к очереди.
	// Это иногда хочется (изоляция critical от default), а иногда нет: если задача
	// по ошибке встала в две очереди — отправится дважды. С TaskID то же самое
	// (см. часть 1), поэтому очередь лучше закреплять за типом в конструкторе.
	task4, err := tasks.NewEmailTask(payload)
	require.NoError(t, err)
	_, err = client.Enqueue(task4, asynq.Unique(uniqWindow), asynq.Queue("critical"))
	require.NoError(t, err)
	t.Logf("  Тот же payload, но в critical — OK (Unique scoped к очереди) ✓")
}

// TestUnique_LockLifetime проверяет, когда снимается блокировка Unique.
//
//   - Успешное выполнение снимает её сразу, не дожидаясь конца TTL:
//     ту же задачу можно поставить снова.
//   - Архив блокировку НЕ снимает: пока не истёк TTL, повторный Enqueue той же
//     задачи (например, после фикса бага) получит ErrDuplicateTask. Вернуть
//     такую задачу в работу можно через Inspector (RunArchivedTask) или
//     удалив её из архива.
func TestUnique_LockLifetime(t *testing.T) {
	opt := newRedis(t)
	startServer(t, opt, asynq.Config{Concurrency: 1},
		asynq.HandlerFunc(func(_ context.Context, task *asynq.Task) error {
			if task.Type() == "test:broken" {
				return fmt.Errorf("bad payload: %w", asynq.SkipRetry)
			}
			return nil
		}))
	client := newClient(t, opt)
	insp := newInspector(t, opt)
	const ttl = time.Hour

	okInfo, err := client.Enqueue(asynq.NewTask("test:ok", []byte("42")), asynq.Unique(ttl))
	require.NoError(t, err)
	waitDeleted(t, insp, okInfo.Queue, okInfo.ID)
	_, err = client.Enqueue(asynq.NewTask("test:ok", []byte("42")), asynq.Unique(ttl))
	require.NoError(t, err, "после успеха блокировка Unique снята, хотя TTL ещё час")
	t.Logf("После успешного выполнения — повторный Enqueue OK ✓")

	brokenInfo, err := client.Enqueue(asynq.NewTask("test:broken", []byte("42")), asynq.Unique(ttl))
	require.NoError(t, err)
	waitState(t, insp, brokenInfo.Queue, brokenInfo.ID, asynq.TaskStateArchived)
	_, err = client.Enqueue(asynq.NewTask("test:broken", []byte("42")), asynq.Unique(ttl))
	require.ErrorIs(t, err, asynq.ErrDuplicateTask, "архив блокировку не снимает")
	t.Logf("После ухода в архив — повторный Enqueue: %v ✓", err)
}

// TestUnique_TaskIDAfterCompletion: TaskID защищает, только пока задача
// существует в Redis. Без Retention выполненная задача удаляется сразу, и тот
// же ID снова свободен — повторная постановка выполнит работу ещё раз. С
// Retention задача хранится в completed, и ID занят до конца срока хранения.
func TestUnique_TaskIDAfterCompletion(t *testing.T) {
	opt := newRedis(t)
	startServer(t, opt, asynq.Config{Concurrency: 1}, asynq.HandlerFunc(noop))
	client := newClient(t, opt)
	insp := newInspector(t, opt)

	_, err := client.Enqueue(asynq.NewTask("test:report", nil), asynq.TaskID("report:1"))
	require.NoError(t, err)
	waitDeleted(t, insp, "default", "report:1")
	_, err = client.Enqueue(asynq.NewTask("test:report", nil), asynq.TaskID("report:1"))
	require.NoError(t, err, "без Retention ID освобождается сразу после выполнения")
	t.Logf("Без Retention: после выполнения тот же TaskID принимается снова")

	_, err = client.Enqueue(asynq.NewTask("test:report", nil),
		asynq.TaskID("report:2"), asynq.Retention(time.Hour))
	require.NoError(t, err)
	waitState(t, insp, "default", "report:2", asynq.TaskStateCompleted)
	_, err = client.Enqueue(asynq.NewTask("test:report", nil), asynq.TaskID("report:2"))
	require.ErrorIs(t, err, asynq.ErrTaskIDConflict, "с Retention ID занят, пока задача в completed")
	t.Logf("С Retention: %v", err)
}
