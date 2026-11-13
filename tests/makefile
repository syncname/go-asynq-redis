# Тесты к статье «Asynq: как сделать из Redis production-ready очередь задач».
# Настоящий Redis не нужен: каждый тест поднимает свой miniredis.
#
#   make                                  — список целей
#   make test-retry                       — тесты одного раздела статьи
#   make run T=TestRetry_DelayFuncArguments — один тест (или регулярка)
#   make test-all RACE=1                  — всё под race detector
#
# Из корня репозитория: make -C tests test-retry

GO  ?= go
PKG := .

# -v печатает t.Logf (в логах основная наглядность тестов), -count=1 отключает
# кэш результатов, -timeout не даёт зависшему тесту висеть 10 минут.
TESTFLAGS ?= -v -count=1 -timeout 2m
ifdef RACE
TESTFLAGS += -race
endif

.DEFAULT_GOAL := help

# go test -run с регулярным выражением, под которое не попал ни один тест,
# печатает "no tests to run" и завершается успешно — опечатка в имени теста
# выглядела бы как зелёный прогон. Поэтому сначала проверяем через -list,
# что регулярка что-то находит.
define run_tests
	@if ! $(GO) test -list '$(1)' $(PKG) | grep -q '^Test'; then \
		echo "make: под -run '$(1)' не попал ни один тест" >&2; exit 1; fi
	$(GO) test $(TESTFLAGS) -run '$(1)' $(PKG)
endef

.PHONY: help test-all test-basic test-schedule test-retry test-timeout test-precision \
	test-queue test-priority test-unique test-features test-reliability test-ops \
	run list vet

help: ## показать список целей
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-17s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test-all: ## все тесты
	$(GO) test $(TESTFLAGS) $(PKG)

test-basic: ## 4.   задача, handler, жизненный цикл, Retention
	$(call run_tests,^(TestEmailTask|TestLifecycle)_)

test-schedule: ## 5.1  ProcessIn / ProcessAt, секундная точность
	$(call run_tests,^TestSchedule_)

test-retry: ## 5.2  MaxRetry, SkipRetry, RetryDelayFunc
	$(call run_tests,^TestRetry_)

test-timeout: ## 5.3  Timeout / Deadline, handler, игнорирующий ctx
	$(call run_tests,^TestTimeout_)

test-precision: ## 5.3  только потеря точности Timeout / Deadline
	$(call run_tests,^TestTimeout_SecondPrecision$$)

test-queue: ## 5.4  маршрутизация: очереди, ServeMux, веса
	$(call run_tests,^TestRouting_)

test-priority: ## 5.4  только веса и StrictPriority
	$(call run_tests,^TestRouting_(PriorityWeights|StrictPriority)$$)

test-unique: ## 5.5  Unique и TaskID
	$(call run_tests,^TestUnique_)

test-features: ## 5.   весь раздел «Основные возможности»
	$(call run_tests,^(TestSchedule|TestRetry|TestTimeout|TestRouting|TestUnique)_)

test-reliability: ## 6.   IsFailure, RevokeTask
	$(call run_tests,^TestReliability_)

test-ops: ## 7–8. пауза очереди, архив, ErrorHandler
	$(call run_tests,^TestOps_)

run: ## один тест или регулярка: make run T=TestRetry_DelayFuncArguments
	$(if $(T),,$(error укажите тест: make run T=ИмяТеста))
	$(call run_tests,$(T))

list: ## список всех тестов
	@$(GO) test -list . $(PKG) | grep '^Test'

vet: ## go vet по тестам
	$(GO) vet $(PKG)
