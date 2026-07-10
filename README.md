# go-cdr-fraud-detector

Akan **CDR (Call Detail Record)** verisinden **gerçek zamanlı fraud (dolandırıcılık) tespiti** yapan, Go ile yazılmış event-driven bir sistem. Telekomda genelde pahalı, kapalı-kutu kurumsal ürünlerle çözülen bir problemin çekirdeğini Kafka ve temiz mikroservislerle açık şekilde çözer.

> 🚧 **Aktif geliştirme.** Şu an çalışan: **velocity fraud kuralı** — uçtan uca, testli, CI'lı.

## Durum

| Yetenek | Durum |
|---|---|
| Event-driven pipeline (Kafka, KRaft) | ✅ |
| Velocity kuralı (Redis kayan pencere) | ✅ |
| Idempotency + manuel offset commit | ✅ |
| Flag'leri HTTP'de sunma (`read-api`) | ✅ |
| CI (gofmt · build · vet · test) | ✅ |
| gRPC enrichment + impossible-travel kuralı | 🔜 Faz 2 |
| IRSF kuralı | 🔜 Faz 3 |
| Gözlemlenebilirlik (Prometheus/Grafana) | 🔜 Faz 4 |

## Nasıl çalışır

```
generator ──▶ Kafka (cdr.raw) ──▶ fraud ──▶ Kafka (cdr.fraud.alert) ──▶ read-api ──▶ HTTP
 sentetik      3 partition          velocity      alert                   Postgres      /alerts
 CDR + burst   msisdn-key'li        (Redis)                               (idempotent)
```

| Servis | Görev |
|---|---|
| **generator** | Sentetik CDR üretir + periyodik "velocity burst" (dolandırıcı senaryosu) enjekte eder |
| **fraud** | `cdr.raw`'ı tüketir; abone başına Redis kayan pencerede çağrı sayar; eşik aşılınca alert üretir |
| **read-api** | Alert'leri Postgres'e (idempotent) yazar; `GET /alerts`, `GET /healthz` sunar |
| **subscriber** | _(Faz 2)_ gRPC statik referans servisi — şimdilik iskelet |

Altyapı: **Kafka (KRaft)** · **Postgres** · **Redis**.

## Hızlı demo

Gerekli: Docker + Docker Compose.

```bash
make up        # her şeyi tek komutla ayağa kaldırır
```

~15 saniye içinde generator ilk dolandırıcılık burst'ünü enjekte eder, fraud yakalar. Gör:

```bash
curl -s localhost:8090/alerts
# {"count":4,"alerts":[
#   {"caller_msisdn":"+905...","rule":"velocity",
#    "evidence":"12 calls within 60s (threshold 12)","score":1, ...}
# ]}
```

```bash
make logs      # canlı loglar — fraud'un "FRAUD" satırlarını gör
make down      # her şeyi kapat
```

## Tasarım notları

- **Partition-by-key:** olaylar `caller_msisdn` ile key'lenir → bir abonenin tüm çağrıları aynı partition/consumer'a düşer; böylece kayan pencere consumer'lar ölçeklenince bile tutarlı kalır.
- **Idempotency:** kayan pencerenin ZSET üyesi `record_id` olduğundan tekrar teslim edilen olay sayacı şişirmez; alert'ler Postgres'e `ON CONFLICT DO NOTHING` ile yazılır.
- **Manuel commit:** offset yalnızca işlem bittikten sonra ilerler → _at-least-once_ teslim + idempotency = pratikte _effectively-once_.

## Geliştirme

```bash
make build     # go build ./...
make test      # testler
make lint      # gofmt + vet
make help      # tüm komutlar
```

## Lisans

[MIT](LICENSE)
