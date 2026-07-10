# go-cdr-fraud-detector

Akan **CDR (Call Detail Record)** verisinden **gerçek zamanlı fraud (dolandırıcılık) tespiti** yapan, Go ile yazılmış event-driven bir sistem. Telekomda genelde pahalı, kapalı-kutu kurumsal ürünlerle çözülen bir problemin çekirdeğini; Kafka, gRPC ve temiz mikroservislerle açık şekilde çözer.

> 🚧 **Aktif geliştirme — Faz 0 (iskelet).** Tüm yol haritası: [PLAN.md](PLAN.md).

## Mimari (özet)

```
generator ──▶ Kafka (cdr.raw) ──▶ fraud-detector ──▶ cdr.fraud.alert ──▶ read-api
                                       └── gRPC ──▶ subscriber-service
```

| Servis | Görev |
|---|---|
| **generator** | Sentetik CDR üretir (+ "kirli trafik" fraud senaryoları) |
| **fraud-detector** | 3 kural (velocity / impossible-travel / IRSF), stateful (Redis) |
| **subscriber-service** | gRPC statik referans: plan / tarife / hücre→coğrafya |
| **read-api** | Flag'lenen çağrıları ve canlı istatistiği HTTP'de sunar |

Altyapı: **Kafka (KRaft)** · **Postgres** · **Redis**.

## Çalıştırma

Gerekli: Docker + Docker Compose.

```bash
make up        # infra (Kafka/Postgres/Redis) + servisler
make ps        # durum
make logs      # loglar
make down      # kapat (volume dahil)
```

Geliştirme:

```bash
make build     # go build ./...
make test      # testler
make lint      # gofmt + vet
make help      # tüm komutlar
```

## Lisans

[MIT](LICENSE)
