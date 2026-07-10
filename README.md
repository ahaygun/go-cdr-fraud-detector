# go-cdr-fraud-detector

Akan **CDR (Call Detail Record)** verisinden **gerçek zamanlı fraud (dolandırıcılık) tespiti** yapan, Go ile yazılmış event-driven bir sistem. Telekomda genelde pahalı, kapalı-kutu kurumsal ürünlerle çözülen bir problemin çekirdeğini Kafka ve temiz mikroservislerle açık şekilde çözer.

> 🚧 **Aktif geliştirme.** Şu an çalışan: **3 fraud kuralı** (velocity · impossible-travel · IRSF, gRPC enrichment'lı) — uçtan uca, testli, CI'lı.

## Durum

| Yetenek | Durum |
|---|---|
| Event-driven pipeline (Kafka, KRaft) | ✅ |
| Velocity kuralı (Redis kayan pencere) | ✅ |
| Impossible-travel kuralı (Haversine + son-konum) | ✅ |
| IRSF kuralı (premium harcama penceresi) | ✅ |
| gRPC enrichment (subscriber-service) | ✅ |
| Idempotency + manuel offset commit | ✅ |
| Flag'leri HTTP'de sunma (`read-api`) | ✅ |
| CI (gofmt · build · vet · test) | ✅ |
| Gözlemlenebilirlik (Prometheus + Grafana) | ✅ |
| Kubernetes + KEDA autoscaling | 🔜 opsiyonel (Faz 5) |
| Yük testi (k6) + sayılar | 🔜 opsiyonel (Faz 6) |

## Nasıl çalışır

```
generator ──▶ Kafka (cdr.raw) ──▶ fraud ──▶ Kafka (cdr.fraud.alert) ──▶ read-api ──▶ HTTP
 sentetik      3 partition        │  velocity · impossible-travel · IRSF   Postgres      /alerts
 CDR + fraud   msisdn-key'li      └── gRPC ──▶ subscriber (cell→geo, tarife) (idempotent)
 senaryoları
```

| Servis | Görev |
|---|---|
| **generator** | Sentetik CDR üretir + periyodik fraud senaryoları (velocity burst, impossible-travel, IRSF) enjekte eder |
| **fraud** | `cdr.raw`'ı tüketir; **velocity**, **impossible-travel** ve **IRSF** (premium harcama penceresi) kurallarını uygular; abone başına dedup'lı alert üretir |
| **subscriber** | gRPC referans servisi — hücre→coğrafya (`GetCell`) ve destinasyon tarifesi (`GetTariff`) sunar; fraud enrichment için çağırır |
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
# {"count":N,"alerts":[
#   {"caller_msisdn":"+905...","rule":"velocity",
#    "evidence":"12 calls within 60s (threshold 12)", ...},
#   {"caller_msisdn":"+905...","rule":"impossible_travel",
#    "evidence":"7930 km in 1s → 28548026 km/h (max 1000)", ...}
# ]}
```

```bash
make logs      # canlı loglar — fraud'un "FRAUD" satırlarını gör
make down      # her şeyi kapat
```

## Gözlemlenebilirlik

Prometheus + Grafana ayrı bir compose profilinde — çekirdeği bozmadan üste eklenir:

```bash
make up-observability   # çekirdek + Prometheus + Grafana + kafka-exporter
```

Grafana: **http://localhost:3001** (anonim, login yok). **"CDR Fraud Detection"** dashboard'u canlı gösterir:

- **throughput** — üretilen / işlenen olay/s
- **kural bazında fraud alert/s** — velocity · impossible-travel · IRSF
- **Kafka consumer lag** — `kafka-exporter`'dan (KEDA autoscaling'in de temeli)
- özet sayaçlar

Her Go servisi `/metrics` (`:9100`) sunar; Prometheus 5 saniyede bir toplar.

## Tasarım notları

- **Partition-by-key:** olaylar `caller_msisdn` ile key'lenir → bir abonenin tüm çağrıları aynı partition/consumer'a düşer; böylece kayan pencere consumer'lar ölçeklenince bile tutarlı kalır.
- **Idempotency:** kayan pencerenin ZSET üyesi `record_id` olduğundan tekrar teslim edilen olay sayacı şişirmez; alert'ler Postgres'e `ON CONFLICT DO NOTHING` ile yazılır.
- **Manuel commit:** offset yalnızca işlem bittikten sonra ilerler → _at-least-once_ teslim + idempotency = pratikte _effectively-once_.
- **Senkron enrichment (gRPC):** impossible-travel için fraud, her çağrının hücre koordinatını `subscriber-service`'ten gRPC ile (timeout'lu) çeker. subscriber-service düşse bile velocity ve pipeline çalışmaya devam eder — enrichment zarifçe devre dışı kalır (graceful degradation).
- **Alert de-dup:** aynı abone + kural için pencere başına en fazla bir alert (Redis cooldown bayrağı) → bir burst 4-5 değil, **tek** alert üretir. Bayrak yalnızca başarılı emit'ten sonra konur, böylece emit hatası retry'da kaybolmaz.

## Geliştirme

```bash
make build     # go build ./...
make test      # testler
make lint      # gofmt + vet
make help      # tüm komutlar
```

## Lisans

[MIT](LICENSE)
