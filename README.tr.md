# go-cdr-fraud-detector

*[English](README.md) · Türkçe*

Akan **CDR (Call Detail Record)** verisi üzerinde **gerçek zamanlı telekom fraud
(dolandırıcılık) tespiti** — Go ile. Event-driven bir pipeline çağrıları
Kafka'dan alır ve üç bağımsız fraud kuralını üzerlerinde çalıştırır; şüpheli
aktiviteyi milisaniyeler içinde flag'ler — temiz, ayrı ayrı deploy edilebilir
mikroservislerin arkasında.

> Telekom operatörleri fraud'u genelde pahalı, kapalı-kutu kurumsal sistemlerle
> tespit eder. Bu proje o problemin çekirdeğine Go-native, açık bir bakış:
> akan CDR trafiğinde fraud'u temiz ve gözlemlenebilir şekilde yakalamak.

## Canlı demo — karar motorunu tarayıcında dene

**[ahaygun.github.io/go-cdr-fraud-detector](https://ahaygun.github.io/go-cdr-fraud-detector/)** — üç fraud kuralı **WebAssembly**'ye derlenip, üretip enjekte edebileceğin sentetik çağrılar üzerinde canlı çalışır. Eşikleri kaydır, fraud'un gerçek zamanda yakalanışını izle.

**Gerçek kural kodunu** (`internal/rules`) değiştirmeden çalıştırır; yalnızca abone-başı state deposu (pipeline'da Redis) bellek-içi map'lerle değiştirilir ve enrichment yerel kataloglardan gelir. Yani bu, **karar motorunun** demosudur — dağıtık sistemin değil; tarayıcıda Kafka, Redis ya da mikroservis yoktur. Tam event-driven pipeline'ı README'nin geri kalanı anlatır.

## Öne çıkanlar

- **Event-driven pipeline** — generator CDR'ları Kafka'ya üretir; bağımsız
  tüketiciler işler. Üretici ve tüketiciler ayrıştırılmış, böylece fraud servisi
  kendi başına ölçeklenir ve çöker.
- **Üç fraud kuralı, üç teknik** — üç eşik kontrolü değil: **velocity** (stream
  windowing), **impossible-travel** (stateful son-konum) ve **IRSF** (enrichment
  + harcama penceresi). Her biri gerçek ve farklı bir fraud tipini hedefler.
- **Senkron gRPC enrichment** — fraud servisi her kaydı bir gRPC referans
  servisiyle (hücre → coğrafya, destinasyon tarifesi) zenginleştirir ve o servis
  düşse bile **zarifçe devam eder**: diğer kurallar ve pipeline çalışmaya devam
  eder.
- **Tekrar teslimde doğru** — abone-bazlı partition, manuel offset commit ve
  `record_id` idempotency, at-least-once teslimi *pratikte* effectively-once'a
  çevirir. Abone başına pencere başına tek alert — her çağrıya bir tane değil.
- **Gözlemlenebilir** — Prometheus metrikleri ve provision'lı bir Grafana
  dashboard'u (throughput, kural bazında alert, consumer lag).
- **Kubernetes-native ölçekleme** — bir Helm chart tüm stack'i deploy eder;
  **KEDA** fraud tüketicisini Kafka lag'ine göre ölçekler (1 → 3 replica).
- **Ölçülmüş** — replica başına ~890 olay/s, p99 ~25 ms; sayılar tekrar
  üretilebilir (bkz. [Performans](#performans)).

## Mimari

```
 generator ──▶ Kafka ─────▶ fraud ─────▶ Kafka ─────▶ read-api ──▶ HTTP /alerts
 (sentetik     (cdr.raw,    │ 3 kural     (cdr.fraud   (Postgres,
  CDR +         msisdn-     │ + Redis      .alert)      idempotent)
  fraud         key'li,     │ state)
  senaryoları)  3 part)     │
                            └─ gRPC ─▶ subscriber-service (hücre→coğrafya · tarife)
```

**Uygun yerde async, uygun yerde sync:** olay akışı Kafka üzerinden; kayıt-başı
enrichment senkron bir gRPC çağrısı. İki iletişim modeli, her biri doğru yerde.

| Servis | Görev |
|---|---|
| **generator** | Sentetik CDR üretir + fraud senaryoları enjekte eder (velocity burst, impossible-travel sıçraması, IRSF spike) |
| **fraud** | `cdr.raw`'ı tüketir; üç kuralı Redis state üzerinde çalıştırır; `cdr.fraud.alert`'e alert üretir |
| **subscriber-service** | gRPC referans veri — hücre → coğrafya (`GetCell`) ve destinasyon tarifesi (`GetTariff`) |
| **read-api** | Alert'leri Postgres'e (idempotent) yazar; `GET /alerts` ve `/healthz` sunar |

Altyapı: **Kafka (KRaft)** · **Postgres** · **Redis**.

## Fraud kuralları

Her kural farklı ve gerçek bir fraud tipini yakalar, o yüzden her biri farklı bir
teknik gerektirir — mesele bu; bir eşiğin üç varyasyonu değil.

| Kural | Yakaladığı fraud | Teknik |
|---|---|---|
| **Velocity** | SIM-box / yüksek hacim istismarı | Abone başına Redis kayan-pencere sayımı |
| **Impossible-travel** | klonlanmış SIM / hesap ele geçirme | son-konum hücresi (Redis) + Haversine mesafe vs. geçen süre |
| **IRSF** | International Revenue Share Fraud | pencerede biriken premium-destinasyon harcaması (tarife gRPC ile) |

Generator her senaryoyu bir zamanlayıcıyla enjekte eder; taze bir koşu üçünü de
bir dakika içinde flag'ler.

## Çalıştırma

Gerekli: Docker + Docker Compose.

```bash
make up                        # tüm çekirdek, tek komut
curl -s localhost:8090/alerts  # flag'li çağrılar (JSON)
make logs                      # fraud'un enjekte senaryoları yakalayışını izle
make down
```

~15 saniye içinde generator ilk fraud senaryosunu enjekte eder, fraud servisi
flag'ler ve alert `read-api`'de görünür.

## Tasarım kararları

- **Abone başına partition** — olaylar `caller_msisdn` ile key'lenir, böylece bir
  abonenin tüm çağrıları tek partition ve tek consumer'a düşer. Stateful kurallar
  tutarlı kalır, sıra korunur ve fraud servisi (partition sayısına kadar) abone
  state'ini bölmeden yatay ölçeklenir.
- **Effectively-once** — Kafka at-least-once verir; fraud consumer offset'i
  manuel (yalnızca işledikten sonra) commit eder, `record_id` ile dedup yapar ve
  kayan pencerenin üyesi olarak `record_id` kullanır, böylece tekrar teslim edilen
  bir kayıt sayacı şişiremez. Alert'ler `ON CONFLICT DO NOTHING` ile yazılır.
- **Poison mesaj partition'ı bloklamaz** — bir kayıt sınırlı sayıda yeniden
  denenir; hiç işlenemiyorsa hata nedeniyle birlikte bir dead-letter topic'ine
  (`cdr.dlq`) yönlendirilir, sonra offset commit'lenip devam edilir. Dead-letter
  yazımının kendisi başarısız olursa, kaydı düşürmek yerine offset commit'lenmez
  (tekrar teslim edilir).

- **Önce emit, sonra state** — alert, kuralın Redis state'i ilerlemeden *önce*
  üretilir, böylece başarısız bir emit kaybolmak yerine retry edilebilir.
  Alert'ler `(kural, abone)` başına pencere başına dedup'lanır; burst tek alert
  üretir, sel değil.
- **Cache'li gRPC enrichment, zarif düşüş** — impossible-travel ve IRSF referans
  veri (hücre coğrafyası, tarife) ister; bunlar timeout'lu gRPC ile alınıp
  kısa-TTL'li bir cache-aside katmanında tutulur (veri statik). Steady-state
  lookup'lar tel üstüne çıkmaz — pipeline'ın ana darboğazı buydu — ve cache'li
  key'ler kısa bir enrichment kesintisi boyunca kuralları çalışır tutar. Lookup
  cache'te yoksa ve `subscriber-service` düştüyse o kural atlanır; velocity ve
  pipeline devam eder.
- **Bilinçli async vs sync** — Kafka yüksek-hacimli olay akışını ayrıştırır; gRPC
  senkron kayıt-başı lookup'ı sunar. Ayrım bilinçli — bir mülakatçının
  sorgulayabileceği türden bir karar.

## Gözlemlenebilirlik

```bash
make up-observability   # Prometheus + Grafana + kafka-exporter ekler
# Grafana: http://localhost:3001  (anonim, login yok)
```

Her servis `:9100`'de Prometheus metrikleri sunar; provision'lı Grafana
dashboard'u throughput, kural bazında fraud alert ve Kafka consumer lag'i canlı
gösterir:

![Grafana dashboard](docs/grafana-dashboard.png)

## Kubernetes + autoscaling

```bash
make k8s-up      # kind cluster + KEDA + Helm chart
make k8s-load    # lag üret → KEDA fraud'u 1 → 3'e ölçekler
kubectl get hpa -w
make k8s-down
```

Helm chart (`deploy/helm/cdr`) tüm stack'i health probe'larıyla deploy eder. Bir
**KEDA `ScaledObject`** fraud deployment'ını `cdr.raw` consumer lag'ine göre
ölçekler (min 1, max 3 = partition sayısı); olaylar abone başına key'lendiği için
ölçeklenen replica'lar abone-state tutarlılığını korur. Yakalanan kanıt:
[`docs/k8s-autoscaling.txt`](docs/k8s-autoscaling.txt).

> ⚠️ Local bir kind cluster — bu, "production Kubernetes operasyonu" değil,
> *K8s'e Helm ile deploy + KEDA ile lag-tabanlı autoscaling* gösterimidir.

## Performans

Docker Compose'da, tek makinede (Apple Silicon, Docker ~11 CPU / 8 GB), tek
`fraud` replica ile ölçüldü — `make loadtest` ve `k6 run loadtest/read-api.js`
ile tekrar üretilebilir:

| Ölçüm | Sonuç |
|---|---|
| Pipeline throughput (fraud, doyurulmuş) | **~1.700 olay/s** — enrichment cache öncesi ~890'dan (≈1.9×) |
| Pipeline latency, 300 olay/s'de (üretim → işlenme) | p50 ~6 ms · **p99 ~25 ms** |
| read-api HTTP (`GET /alerts`, 50 VU, k6) | **~12.000 istek/s** · p95 ~7 ms · %0 hata |

Replica başına throughput'u *önceden* fraud servisinin kayıt-başı iki senkron
gRPC enrichment çağrısı + Redis işlemleri sınırlıyordu — producer tek başına
~533k/s sürdürüyor, yani maliyet *ingest* değil, *işleme*. O statik enrichment'ı
cache'lemek (cache-aside; Tasarım kararları'na bak) gRPC round-trip'lerini hot
path'ten çıkardı ve throughput'u kabaca ikiye katladı (~890 → ~1.700 olay/s);
darboğaz artık Redis. Gerisini KEDA çözer: yük altında fraud 1 → 3'e ölçeklenip
replica-başı kapasiteyi kabaca üçe katlar.

> ⚠️ Tek makine, local sayılar — dağıtık benchmark değil; makine ve komutlar
> şeffaf, tekrar üretebilirsin.

## Test & CI

```bash
make test   # unit testler — fraud kuralları saf ve table-driven
make lint   # gofmt + vet
```

GitHub Actions her push'ta gofmt, build, vet ve testleri koşar.

## Dizin yapısı

```
cmd/loadgen         async Kafka yük üreteci (throughput/latency testleri)
cmd/playground      fraud kuralları WebAssembly'ye derlenmiş (tarayıcı-içi demo)
internal/cdr        olay şeması (CDR, FraudAlert)
internal/stream     Kafka yardımcıları (key'li producer, manuel-commit consumer)
internal/rules      üç fraud kuralı (saf, table-tested)
internal/geo        hücre → coğrafya katalogu + Haversine
internal/tariff     destinasyon tarife katalogu
internal/platform   log, config, metrik/health sunucusu, graceful shutdown
services/           generator · fraud · subscriber · read-api
proto/              gRPC sözleşmesi (subscriber Reference servisi)
deploy/             Dockerfile, docker-compose, Helm chart, observability
loadtest/           k6 script'i
web/                tarayıcı-içi demo sayfası (tasarım kabuğu + wasm yükleyici)
docs/               GitHub Pages için derlenmiş demo (make wasm) + kanıt dosyaları
```

## Lisans

[MIT](LICENSE) © Ahmet Hasan Aygün
