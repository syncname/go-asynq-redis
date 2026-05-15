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