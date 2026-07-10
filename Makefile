.DEFAULT_GOAL := help
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help build vet test lint tidy proto up up-infra up-observability down logs ps clean

help: ## Bu yardımı göster
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Tüm servisleri derle
	go build ./...

vet: ## go vet
	go vet ./...

test: ## Testleri çalıştır
	go test ./...

lint: ## gofmt + vet (golangci-lint kuruluysa onu da)
	@test -z "$$(gofmt -l .)" || (echo "gofmt gerekli:"; gofmt -l .; exit 1)
	go vet ./...
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run || echo "(golangci-lint yok; gofmt+vet ile geçildi)"

tidy: ## go mod tidy
	go mod tidy

proto: ## proto'dan Go + gRPC kodu üret (buf gerekir)
	buf generate

up: ## Tüm sistemi ayağa kaldır (infra + servisler)
	$(COMPOSE) up --build -d

up-observability: ## Çekirdek + gözlemlenebilirlik (Prometheus + Grafana → http://localhost:3001)
	$(COMPOSE) --profile observability up --build -d

up-infra: ## Sadece altyapı (kafka, postgres, redis)
	$(COMPOSE) up -d kafka postgres redis

down: ## Her şeyi indir (observability + volume dahil)
	$(COMPOSE) --profile observability down -v

logs: ## Logları izle
	$(COMPOSE) logs -f

ps: ## Servis durumları
	$(COMPOSE) ps

clean: ## Derleme artıklarını temizle
	rm -rf bin
	go clean
