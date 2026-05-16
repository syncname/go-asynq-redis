.PHONY: test-all test-schedule test-retry test-timeout test-queue test-unique

# Запуск всех тестов в проекте
test-all:
	go test -v ./...

# 5.1 Delayed tasks: тест отложенного выполнения
test-schedule:
	go test -v -run ^TestSchedules_RelativeAndAbsolute$$ ./...

# 5.2 Retries: тест архивации после превышения лимита попыток
test-retry:
	go test -v -run ^TestMaxRetry_ArchivesAfterLimit$$ ./...

# 5.3 Timeout и Deadline: тест отмены контекста по таймауту
test-timeout:
	go test -v -run ^TestTimeout_CancelsContext$$ ./...

# 5.4 Маршрутизация по типам задач: тест распределения по очередям
test-queue:
	go test -v -run ^TestQueueOption_RoutesTask$$ ./...

# 5.5 Unique tasks: тест защиты от дубликатов по TaskID
test-unique:
	go test -v -run ^TestTaskID_RejectsDuplicate$$ ./...