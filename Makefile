.PHONY: generate test check check-integration db-up db-down run fake-worker signal-image telegram-image deploy deploy-status deploy-logs deploy-down

DEV_DATABASE_URL := postgres://say:say@localhost:54329/say?sslmode=disable
DEV_JWT_KEY := development-jwt-key-change-me-000000000
DEV_LOGIN_KEY := development-login-key-change-me-000000

generate:
	./scripts/generate.sh

test:
	go test ./...

check: generate
	go test -race ./...
	go vet ./...
	npm --prefix clients/typescript run check
	npm --prefix clients/typescript run validate:schemas
	npm --prefix clients/typescript audit --audit-level=high
	npm --prefix workers/telegram run check
	npm --prefix workers/telegram test
	npm --prefix workers/telegram audit --audit-level=high

check-integration: db-up
	SAY_TEST_DATABASE_URL='$(DEV_DATABASE_URL)' go test -race ./...

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down

run: db-up
	SAY_ENV=development \
	SAY_DATABASE_URL='$(DEV_DATABASE_URL)' \
	SAY_JWT_SIGNING_KEY='$(DEV_JWT_KEY)' \
	SAY_LOGIN_HASH_KEY='$(DEV_LOGIN_KEY)' \
	SAY_AUTH_DEBUG_RETURN_CODE=true \
	go run ./cmd/say-api

fake-worker:
	go run ./cmd/fake-worker

signal-image:
	docker build -f Dockerfile.signal -t say-signal:dev .

telegram-image:
	docker build -f Dockerfile.telegram -t say-telegram:dev .

deploy:
	./scripts/deploy.sh

deploy-status:
	docker compose --env-file .env.production -f deploy/production.compose.yaml ps -a

deploy-logs:
	docker compose --env-file .env.production -f deploy/production.compose.yaml logs -f --tail=200

deploy-down:
	docker compose --env-file .env.production -f deploy/production.compose.yaml down
