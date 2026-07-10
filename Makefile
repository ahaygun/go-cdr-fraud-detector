.DEFAULT_GOAL := help
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help build vet test lint tidy proto up up-infra up-observability down logs ps clean \
        k8s-images k8s-up k8s-load k8s-unload k8s-down loadtest

KIND_CLUSTER := cdr

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

loadtest: ## Pipeline'ı doyur (RATE=0) — throughput/p99'u Grafana/Prometheus'tan oku
	RATE=$${RATE:-0} DURATION=$${DURATION:-30} go run ./cmd/loadgen

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

k8s-images: ## Servis image'larını derle + kind'e yükle
	@for s in generator fraud subscriber read-api; do \
		docker build -q --build-arg SERVICE=$$s -f deploy/Dockerfile -t cdr/$$s:dev . >/dev/null && echo "built $$s"; \
	done
	kind load docker-image cdr/generator:dev cdr/fraud:dev cdr/subscriber:dev cdr/read-api:dev --name $(KIND_CLUSTER)

k8s-up: ## kind cluster + KEDA + cdr chart (çekirdeği Kubernetes'e kurar)
	kind create cluster --name $(KIND_CLUSTER)
	$(MAKE) k8s-images
	helm repo add kedacore https://kedacore.github.io/charts
	helm repo update kedacore
	helm install keda kedacore/keda -n keda --create-namespace --wait
	helm install cdr ./deploy/helm/cdr
	@echo "izle: kubectl get pods -w   |   yük: make k8s-load"

k8s-load: ## Lag üret → KEDA fraud'u 1→3 ölçeklendirir (izle: kubectl get hpa -w)
	kubectl scale deployment generator --replicas=6

k8s-unload: ## Yükü kaldır (fraud geri 1'e iner)
	kubectl scale deployment generator --replicas=1

k8s-down: ## kind cluster'ı tamamen sil
	kind delete cluster --name $(KIND_CLUSTER)

clean: ## Derleme artıklarını temizle
	rm -rf bin
	go clean
