package tests

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════
// 5.1b Periodic tasks
// ═══════════════════════════════════════════════════════════════════════════
//
// Расписания ниже используют дескриптор "@every 1s" из robfig/cron, на котором
// построен Scheduler Asynq. У обычного cron-выражения минимальный шаг — минута,
// и тест ждал бы слишком долго.
//
// Server в этих тестах не запускаем: задачи остаются в pending, и по ним видно,
// что именно и сколько раз поставил планировщик.
//
// Каждую постановку логируем через SchedulerOpts.PostEnqueueFunc — этот хук
// Asynq вызывает после каждого тика расписания. Так в выводе теста видно, кто,
// когда и какую задачу поставил.

// newScheduler создаёт Scheduler поверх того же Redis и останавливает его в
// конце теста. Возвращает идемпотентную stop-функцию: иногда планировщик нужно
// остановить раньше, чтобы зафиксировать счётчики.
func newScheduler(t *testing.T, opt asynq.RedisConnOpt, opts *asynq.SchedulerOpts) (*asynq.Scheduler, func()) {
	t.Helper()
	if opts == nil {
		opts = &asynq.SchedulerOpts{}
	}
	opts.LogLevel = asynq.WarnLevel // без INFO-строк "Scheduler starting" в выводе теста

	s := asynq.NewScheduler(opt, opts)
	var once sync.Once
	stop := func() { once.Do(s.Shutdown) }
	t.Cleanup(stop)
	return s, stop
}

// nowMs — текущее время с миллисекундами, чтобы в логах был виден интервал
// между тиками.
func nowMs() string { return time.Now().Format("15:04:05.000") }

// TestPeriodic_SchedulerOnlyEnqueues: Scheduler сам задачи не выполняет, он
// только ставит их в очередь по расписанию. Каждый тик — новая задача со своим
// ID, выполнять её будет обычный worker.
//
// Как читать вывод:
//   - "[scheduler] ... тик #N" — момент, когда планировщик поставил задачу;
//   - блок "Состояние очереди" — задачи лежат в pending, active = 0:
//     выполнять их некому, планировщик этого и не делает;
//   - список задач — у каждой свой ID, то есть это разные задачи.
func TestPeriodic_SchedulerOnlyEnqueues(t *testing.T) {
	opt := newRedis(t)
	insp := newInspector(t, opt)

	t.Log("=== Scheduler с расписанием @every 1s, worker не запущен ===")

	var ticks atomic.Int32
	scheduler, stop := newScheduler(t, opt, &asynq.SchedulerOpts{
		PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
			n := ticks.Add(1)
			if err != nil {
				t.Logf("  [scheduler] %s тик #%d: ошибка постановки: %v", nowMs(), n, err)
				return
			}
			t.Logf("  [scheduler] %s тик #%d → поставил задачу %s (type=%s, queue=%s)",
				nowMs(), n, info.ID[:8], info.Type, info.Queue)
		},
	})
	_, err := scheduler.Register("@every 1s", asynq.NewTask("report:daily", nil))
	require.NoError(t, err)
	require.NoError(t, scheduler.Start())
	t.Logf("  %s планировщик запущен, ждём два тика", nowMs())

	require.Eventually(t, func() bool { return ticks.Load() >= 2 },
		5*time.Second, 50*time.Millisecond, "планировщик должен отработать минимум два тика")
	stop() // фиксируем результат: после Shutdown новых тиков не будет

	q, err := insp.GetQueueInfo("default")
	require.NoError(t, err)
	t.Log("=== Состояние очереди default ===")
	t.Logf("  Pending:   %d  ← задачи поставлены и ждут worker", q.Pending)
	t.Logf("  Active:    %d  ← никто их не выполняет: Scheduler только ставит задачи", q.Active)
	t.Logf("  Completed: %d", q.Completed)

	pending, err := insp.ListPendingTasks("default")
	require.NoError(t, err)
	t.Log("=== Задачи в очереди ===")
	ids := make(map[string]struct{}, len(pending))
	for i, ti := range pending {
		t.Logf("  #%d id=%s type=%s state=%s", i+1, ti.ID[:8], ti.Type, ti.State)
		require.Equal(t, "report:daily", ti.Type)
		ids[ti.ID] = struct{}{}
	}

	require.EqualValues(t, ticks.Load(), q.Pending, "сколько тиков, столько и задач")
	require.Zero(t, q.Active, "Scheduler не выполняет задачи")
	require.Len(t, ids, len(pending), "каждый тик создаёт отдельную задачу с новым ID")
}

// TestPeriodic_TwoSchedulersDuplicate: планировщик нельзя запускать в
// нескольких репликах. Каждый экземпляр ведёт своё расписание и ставит свою
// копию задачи, поэтому при двух репликах каждый тик выполнится дважды.
//
// Опция Unique от этого надёжно не защищает: блокировка снимается после
// успешного выполнения (см. TestUnique_LockLifetime), и если worker быстрый,
// вторая реплика успеет поставить дубль. Надёжный вариант — один экземпляр
// планировщика.
//
// Как читать вывод:
//   - "[реплика A]" и "[реплика B]" идут парами с разницей в миллисекунды:
//     на каждый тик расписания приходится по задаче от каждой реплики;
//   - блок "Итог" — задач в очереди вдвое больше, чем тиков.
func TestPeriodic_TwoSchedulersDuplicate(t *testing.T) {
	opt := newRedis(t)
	insp := newInspector(t, opt)

	t.Log("=== Две реплики планировщика, одно расписание @every 1s, worker не запущен ===")

	var enqueuedA, enqueuedB atomic.Int32
	startReplica := func(name string, counter *atomic.Int32) func() {
		s, stop := newScheduler(t, opt, &asynq.SchedulerOpts{
			PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
				if err != nil {
					t.Logf("  [реплика %s] %s ошибка постановки: %v", name, nowMs(), err)
					return
				}
				n := counter.Add(1)
				t.Logf("  [реплика %s] %s тик #%d → поставила задачу %s",
					name, nowMs(), n, info.ID[:8])
			},
		})
		_, err := s.Register("@every 1s", asynq.NewTask("report:daily", nil))
		require.NoError(t, err)
		require.NoError(t, s.Start())
		return stop
	}
	stopA := startReplica("A", &enqueuedA)
	stopB := startReplica("B", &enqueuedB)
	t.Logf("  %s обе реплики запущены, ждём по два тика от каждой", nowMs())

	require.Eventually(t, func() bool {
		return enqueuedA.Load() >= 2 && enqueuedB.Load() >= 2
	}, 5*time.Second, 50*time.Millisecond, "обе реплики должны отработать минимум по два тика")

	// Shutdown дожидается текущих постановок, после него счётчики не меняются.
	stopA()
	stopB()

	q, err := insp.GetQueueInfo("default")
	require.NoError(t, err)
	a, b := enqueuedA.Load(), enqueuedB.Load()
	t.Log("=== Итог ===")
	t.Logf("  Реплика A поставила: %d", a)
	t.Logf("  Реплика B поставила: %d", b)
	t.Logf("  Задач в очереди:     %d = %d + %d", q.Pending, a, b)
	t.Logf("  С одной репликой было бы %d задач: каждая реплика ведёт своё расписание,", a)
	t.Log("  поэтому каждый тик выполнится дважды")

	require.Equal(t, int(a+b), q.Pending, "в очереди лежат копии от обеих реплик")
}

// periodicConfigProvider — источник расписания для PeriodicTaskManager.
// В реальном проекте это таблица в базе или файл конфигурации; здесь — слайс
// под мьютексом, чтобы тест мог подменить расписание на лету.
type periodicConfigProvider struct {
	mu      sync.Mutex
	configs []*asynq.PeriodicTaskConfig
}

func (p *periodicConfigProvider) GetConfigs() ([]*asynq.PeriodicTaskConfig, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.configs, nil
}

func (p *periodicConfigProvider) set(configs ...*asynq.PeriodicTaskConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.configs = configs
}

// TestPeriodic_ManagerReloadsConfig: PeriodicTaskManager раз в SyncInterval
// (по умолчанию 3 минуты) перечитывает расписание из внешнего источника,
// регистрирует новые задачи и снимает удалённые. Перезапускать сервис не нужно.
//
// Как читать вывод:
//   - "[источник]" — момент, когда меняется расписание (как правка в базе);
//   - "[scheduler]" — какие задачи реально ставятся: до смены только
//     report:old, после — только report:new;
//   - строка "Менеджер подхватил изменение через ..." — сколько прошло от правки
//     источника до первой задачи по новому расписанию;
//   - блок "Итог" — report:old после смены больше не появляется.
func TestPeriodic_ManagerReloadsConfig(t *testing.T) {
	opt := newRedis(t)
	insp := newInspector(t, opt)

	const syncInterval = 300 * time.Millisecond
	t.Logf("=== PeriodicTaskManager, SyncInterval = %v (по умолчанию 3 минуты) ===", syncInterval)

	provider := &periodicConfigProvider{}
	provider.set(&asynq.PeriodicTaskConfig{
		Cronspec: "@every 1s",
		Task:     asynq.NewTask("report:old", nil),
	})
	t.Logf("  [источник]  %s расписание: report:old @every 1s", nowMs())

	firstNew := make(chan time.Time, 1)
	mgr, err := asynq.NewPeriodicTaskManager(asynq.PeriodicTaskManagerOpts{
		RedisConnOpt:               opt,
		PeriodicTaskConfigProvider: provider,
		SyncInterval:               syncInterval, // в тесте не ждём 3 минуты
		SchedulerOpts: &asynq.SchedulerOpts{
			LogLevel: asynq.WarnLevel,
			PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
				if err != nil {
					t.Logf("  [scheduler] %s ошибка постановки: %v", nowMs(), err)
					return
				}
				t.Logf("  [scheduler] %s поставил задачу %s (type=%s)", nowMs(), info.ID[:8], info.Type)
				if info.Type == "report:new" {
					select {
					case firstNew <- time.Now():
					default: // нужна только первая задача по новому расписанию
					}
				}
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Start())
	t.Cleanup(mgr.Shutdown)

	countByType := func() map[string]int {
		res := map[string]int{}
		list, err := insp.ListPendingTasks("default", asynq.PageSize(100))
		if err != nil {
			return res // очереди ещё нет
		}
		for _, ti := range list {
			res[ti.Type]++
		}
		return res
	}

	require.Eventually(t, func() bool { return countByType()["report:old"] >= 1 },
		5*time.Second, 50*time.Millisecond, "исходное расписание должно сработать")

	// Меняем расписание в источнике — как если бы его поправили в базе.
	changedAt := time.Now()
	provider.set(&asynq.PeriodicTaskConfig{
		Cronspec: "@every 1s",
		Task:     asynq.NewTask("report:new", nil),
	})
	t.Logf("  [источник]  %s меняем расписание: report:old → report:new", nowMs())

	gotNew := receive(t, firstNew)
	t.Logf("  Менеджер подхватил изменение через %v (до SyncInterval на синхронизацию + ожидание тика)",
		gotNew.Sub(changedAt).Round(10*time.Millisecond))

	// Старое расписание снято: новые report:old больше не появляются.
	oldCount := countByType()["report:old"]
	require.Never(t, func() bool { return countByType()["report:old"] > oldCount },
		2*time.Second, 100*time.Millisecond, "старое расписание должно быть снято")

	final := countByType()
	t.Log("=== Итог ===")
	t.Logf("  report:old: %d  ← после смены расписания больше не ставится", final["report:old"])
	t.Logf("  report:new: %d  ← новое расписание подхвачено без перезапуска", final["report:new"])

	require.Equal(t, oldCount, final["report:old"])
	require.GreaterOrEqual(t, final["report:new"], 1)
}
