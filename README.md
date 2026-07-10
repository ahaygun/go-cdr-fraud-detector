# go-cdr-fraud-detector

Akan **CDR (Call Detail Record)** verisinden **gerçek zamanlı fraud (dolandırıcılık) tespiti** yapan, Go ile yazılmış event-driven bir sistem. Telekomda genelde pahalı, kapalı-kutu kurumsal ürünlerle çözülen bir problemin çekirdeğini Kafka ve temiz mikroservislerle açık şekilde çözer.

> 🚧 **Aktif geliştirme.** Şu an çalışan: **velocity + impossible-travel** kuralları (gRPC enrichment'lı) — uçtan uca, testli, CI'lı.

## Durum

| Yetenek | Durum |
|---|---|
| Event-driven pipeline (Kafka, KRaft) | ✅ |
| Velocity kuralı (Redis kayan pencere) | ✅ |
| Impossible-travel kuralı (Haversine + son-konum) | ✅ |
| gRPC enrichment (subscriber-service) | ✅ |
| Idempotency + manuel offset commit | ✅ |
| Flag'leri HTTP'de sunma (`read-api`) | ✅ |
| CI (gofmt · build · vet · test) | ✅ |
| IRSF kuralı | 🔜 Faz 3 |
| Gözlemlenebilirlik (Prometheus/Grafana) | 🔜 Faz 4 |

## Nasıl çalışır

```
generator ──▶ Kafka (cdr.raw) ──▶ fraud ──▶ Kafka (cdr.fraud.alert) ──▶ read-api ──▶ HTTP
 sentetik      3 partition        │  velocity + impossible-travel        Postgres      /alerts
 CDR + fraud   msisdn-key'li      └── gRPC ──▶ subscriber (cell → geo)   (idempotent)
 senaryoları
```

| Servis | Görev |
|---|---|
| **generator** | Sentetik CDR üretir + periyodik fraud senaryoları (velocity burst, impossible-travel) enjekte eder |
| **fraud** | `cdr.raw`'ı tüketir; **velocity** (Redis kayan pencere) ve **impossible-travel** (gRPC ile hücre→coğrafya + Redis son-konum) kurallarını uygular; alert üretir |
| **subscriber** | gRPC referans servisi — hücre→coğrafya (`GetCell`) sunar; fraud senkron enrichment için çağırır |
| **read-api** | Alert'leri Postgres'e (idempotent) yazar; `GET /alerts`, `GET /healthz` sunar |

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
- **Senkron enrichment (gRPC):** impossible-travel için fraud, her çağrının hücre koordinatını `subscriber-service`'ten gRPC ile (timeout'lu) çeker. subscriber-service düşse bile velocity ve pipeline çalışmaya devam eder — enrichment zarifçe devre dışı kalır (graceful degradation).

## Geliştirme

```bash
make build     # go build ./...
make test      # testler
make lint      # gofmt + vet
make help      # tüm komutlar
```

## Lisans

[MIT](LICENSE)
