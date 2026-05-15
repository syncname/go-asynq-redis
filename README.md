<<<<<<< HEAD
# go-asynq-redis
=======
# Go Asynq Redis Example

Запускаемый пример очереди задач на Go, Asynq и Redis.

## Запуск

1. Запустите Redis:

```bash
docker run --name asynq-redis -p 6379:6379 -d redis:7-alpine
```

Если контейнер уже существует:

```bash
docker start asynq-redis
```

2. Установите зависимости:

```bash
go mod tidy
```

3. Запустите worker:

```bash
go run ./cmd/worker
```

4. Во втором терминале поставьте задачу в очередь:

```bash
go run ./cmd/producer
```

Worker должен вывести лог вида:

```text
email sent: user_id=42 template_id=welcome
```

## Практические примеры

Отдельная практическая лаборатория лежит в `examples/README.md`.

Там есть сценарии:

- delayed tasks;
- retry;
- skip-retry;
- unique tasks;
- priority queues;
- fixed task id;
- inspector;
- scheduler.

Обычно порядок такой:

```bash
go run ./cmd/worker
```

Во втором терминале:

```bash
go run ./cmd/examples/retry
go run ./cmd/examples/inspect
```

## Структура

- `cmd/producer/main.go` - добавляет задачу в очередь.
- `cmd/worker/main.go` - забирает и выполняет задачи.
- `cmd/examples/*` - отдельные учебные сценарии.
- `internal/tasks/*.go` - payload, создание задач и обработчики.
- `examples/README.md` - инструкции по практическим примерам.
- `docs/article.md` - статья по плану из `docs/full.txt`.
- `docs/article_best.md` - улучшенная версия статьи по `docs/adv.txt`.
>>>>>>> d15eb0e (add simple)
