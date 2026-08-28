# CHANGELOG — Data Platform

## v1.17.1 — Bronze Timeframe Partition, MTF Score Coverage (Path C), AU/AG Ticker (Agustus 2026)

Diarahkan oleh `GMI_Decision_Document_v11.docx` (ADR-045, ADR-046, ADR-047)
— ketiga item direkonsiliasi ke satu version bump tunggal sesuai instruksi
Ovi, karena belum ada yang di-mirror ke live repo saat pekerjaan sandbox
ini berjalan.

**ADR-045 — Bronze OHLCV write/scan path gains a timeframe partition.**
`market_ingester.py::_run_symbol()` sebelumnya meneruskan
`bronze_path`/`asset_class` yang hanya di-scope symbol+market+source (tanpa
dimensi timeframe) ke `IncFetchProtocol.resolve_start_date()` dan
`BronzeIngester.write()`. Karena `DEFAULT_TIMEFRAMES=[1D,1W,1M]` memproses
1D lebih dulu, file 1D yang baru ditulis membuat scan sisi-baca melaporkan
`last_date` mendekati hari ini untuk 1W/1M (fetch window ~7 hari, bukan
backfill multi-tahun), DAN idempotency check sisi-tulis same-day (FIX
GD-F08) kemudian menemukan file 1D tersebut dan skip penulisan 1W/1M
sepenuhnya — dikonfirmasi via baca langsung `base_ingester.py`, cocok
persis dengan log `"[Bronze] Idempotent skip"` dari laporan bug live-test
21 Aug 2026. Fix: timeframe dilipat ke kedua path di call site saja
(`asset_class=f"market/ohlcv/{market}/timeframe={tf}"`) — signature
`BronzeIngester`/`IncFetchProtocol` tidak berubah, karena keduanya dipakai
bersama oleh semua domain Bronze non-OHLCV (FRED/BLS/BEA/Treasury/IMF/BIS)
yang tidak punya konsep timeframe.

**Konsekuensial terhadap ADR-045** (tidak disebut eksplisit di
Consequences ADR-045 sendiri, ditemukan empiris saat implementasi — fix
dalam scope, disetujui Ovi mid-session, mengikuti precedent
consequential-edit-discipline proyek ini):

- `_run_context_symbol()` (Layer 2 / GMI-BRZ-001) berbagi konstruksi
  `bronze_path`/`asset_class` yang identik dengan `_run_symbol()`, sehingga
  bug starvation yang sama berlaku — teks ADR-045 sendiri hanya ditulis
  terhadap `_run_symbol()`. Diperbaiki di pass yang sama; jika tidak,
  context anchors Layer 2 (VIX, DXY, global indices, ETF) mewarisi bug
  persis yang coba ditutup ADR-045 untuk Layer 1.
- **Jauh lebih parah, temuan baru murni**: glob baca-Bronze
  `ohlcv_processor.py` (baik `run()` PASS 1 maupun `run_context()`) TIDAK
  PERNAH di-scope timeframe — pattern-nya
  `market/**/symbol={symbol}/**/*.parquet` tanpa segmen tf sama sekali.
  Direproduksi empiris terhadap file Bronze AAPL produksi asli (2.512 baris
  cadence harian, satu-satunya data Bronze yang pernah berhasil persist,
  per starvation bug di atas): pattern pre-fix match IDENTIK untuk
  tf='5m','15m','1H','1D','1W','1M'. Silver belum pernah benar-benar
  dijalankan di repo ini (direktori `data/silver/` belum ada pre-fix),
  jadi belum ada output nyata yang korup — tapi akan terjadi pada run
  `silver_ohlcv` pertama, independen dari ADR-045: setiap timeframe di
  `_RUN_BRONZE_TFS` akan diam-diam menulis baris 1D-cadence yang SAMA di
  bawah label timeframe berbeda. Jika tidak diperbaiki, fix sisi-Bronze
  ADR-045 justru akan memperburuk keadaan, bukan menjadi moot — begitu
  Bronze benar-benar memisahkan 1D/1W/1M/1H (ADR-046 Path C), glob
  rekursif `**` ini akan menyatukan semuanya kembali menjadi satu seri
  multi-cadence yang tercampur per file Silver TF, alih-alih scope ke satu
  partisi Bronze yang benar-benar cocok. Diperbaiki di pass yang sama:
  segmen `timeframe={tf}` ditambahkan ke kedua lokasi glob, cocok persis
  dengan struktur partisi Bronze yang baru. Test regresi baru:
  `test_run_does_not_blend_timeframes_across_bronze_partitions` (dua
  fixture Bronze berukuran berbeda di 1D vs 1W harus menghasilkan dua
  output Silver berukuran berbeda, bukan union identik).
- Temuan ini langsung memberi masukan ke kalkulasi ADR-046: bahkan kondisi
  lama "3 timeframe yang bekerja" (1D/1W/1M) tidak pernah benar-benar 3
  suara independen pre-fix — bug glob Silver berarti ketiganya akan
  membaca blob Bronze yang identik, sehingga nilai non-nol `mtf_score`
  hanya pernah -3/0/+3, tidak pernah mencerminkan divergensi
  mingguan/bulanan yang genuine dari harian.

**ADR-046 (Path C, pilihan eksplisit Ovi dari tiga opsi yang disajikan
GMI v11)** — MTF score coverage secara struktural tidak dapat dicapai
sebelum fix ini: 5m/15m/1H tidak pernah di-fetch ke Bronze, dan karena 4H
disintesis dari Silver 1H saja (`ohlcv_aggregator.py`), 4H juga kosong.
Hanya 1D/1W/1M yang pernah berkontribusi nilai nyata (dan per temuan
konsekuensial di atas, bahkan tidak independen), membatasi `|mtf_score|`
maksimum di 3 sementara `screener.py` mensyaratkan `>=5` DAN
`signal_quality IN ('A','B')` — watchlist secara matematis mustahil
mengembalikan satu baris pun. Path C: 1H di-wire sendirian (bukan 5m/15m)
via konstanta baru `LAYER1_TIMEFRAMES` (`DEFAULT_TIMEFRAMES + ["1H"]`)
diterapkan hanya ke `job_registry.py::_bronze_ohlcv()` — Layer 2 context
(`_bronze_ohlcv_context()`) sengaja tidak disentuh, tetap
`DEFAULT_TIMEFRAMES` saja, sesuai docstring `run_context()` sendiri (tidak
ada consumer Layer 2 yang butuh data context intraday siklus ini). 1H
berkontribusi nilai trend nyata secara langsung DAN membuka sintesis
Silver 1H→4H yang sudah ada, menaikkan kontributor nyata dari 3 ke 5.
`FALLBACK_YEARS["1H"]=2` dan mapping interval yfinance `"1H"→"1h"` sudah
benar sejak awal (tidak perlu diubah — diverifikasi, bukan diasumsikan).

Rentang skor dan batas grade dikalibrasi ulang ke realita 5-kontributor
sesuai instruksi eksplisit Ovi, bukan dibiarkan di nilai 7-timeframe lama
sambil diam-diam melayani lebih sedikit input nyata (persis failure mode
yang coba ditutup ADR-046): `TIMEFRAMES` dipangkas ke
`["1H","4H","1D","1W","1M"]` di `technical_signals.py` maupun
`mtf_alignment.py` (5m/15m dihapus sepenuhnya, bukan di-pad ke 0
permanen — kedua file harus konsisten satu sama lain). Rentang skor baru
-5..+5. Grade baru (D dihapus — C jadi bucket "weak" catch-all yang dulu
peran D): **A ≥ 4, B == 3, C ≤ 2**. `screener.py`'s `MIN_MTF_SCORE`: 5 → 3
(cocok persis dengan batas grade B baru, relasi yang sama seperti 5 lama
terhadap grade B lama). Consumer hilir skema lama diperbarui:
`backtest/engine.py`'s `BacktestConfig.min_mtf_score` default (5 → 3) dan
logic exit/sentinel-default degradasi kualitasnya (`"D"` → `"C"`, bucket
catch-all baru); `config/pipeline_config.py`'s `min_mtf_score_screener`
default (5 → 3, meski field ini saat ini tidak punya reader di source code
— konstanta modul `MIN_MTF_SCORE` milik `screener.py` sendiri yang benar-
benar terpakai); `mtf_alignment.py`'s `get_mtf_summary()` tidak lagi
mengembalikan `grade_D`.

**ADR-047** — cabang commodity `market_ingester.py::_run_symbol()`
sekarang membaca `inst.yfinance_symbol` langsung untuk `market=='commodity'`,
meniru pola Layer 2 yang sudah mapan di `_run_context_symbol()` — alih-alih
`to_api_symbol(inst.raw_symbol, inst.market, primary_src)`, yang cabang
commodity-nya tidak punya override table dan jatuh ke aturan suffix generik
`sym + "=F"`, menghasilkan `AU=F`/`AG=F` yang invalid (dikonfirmasi
terhadap `config/instruments_identity.yaml`, yang sudah menyimpan
`GC=F`/`SI=F` yang benar di `inst.yfinance_symbol` via `InstrumentLoader`).
`CL=F` sudah benar secara kebetulan (prefix ticker WTI cocok dengan simbol
instrumennya) dan tidak terpengaruh. `to_api_symbol()` sendiri tidak
diubah — fix hanya di call site, sesuai Rejected alternative ADR-047
sendiri (commodity override table di dalam `to_api_symbol()` akan
memperkenalkan kembali bentuk dual-source-of-truth yang kemungkinan
menyebabkan bug ini sejak awal).

**Tidak diputuskan di sini / eksplisit di luar scope pass ini**: data
Bronze OHLCV yang sudah ada dari path pre-ADR-045 (non-partitioned)
dibiarkan apa adanya (quarantine-and-rebuild vs. biarkan-dan-cold-start-
forward adalah keputusan Ovi per Consequences ADR-045 sendiri, terpisah
dari code fix ini) — dikonfirmasi via inspeksi langsung Filesystem MCP
bahwa hanya data 1D yang pernah benar-benar persist (`source=yfinance/
symbol=AAPL/year=2026/month=08/`, 2.512 baris, 2016-08-23 s.d.
2026-08-20), sehingga partisi `timeframe={tf}` baru mulai genuinely kosong
untuk setiap simbol pada run berikutnya tanpa perlu migrasi eksplisit.
5m/15m tetap permanen tidak di-fetch by design (Path C, bukan Path A) —
grade A di bawah skema baru (`|score|>=4`) tetap tercapai pada 4/5
kontributor sepakat, tidak perlu 5/5 unanimous.

PATCH bump (bukan MINOR): bug fix ke komponen yang sudah rusak di ketiga
ADR, tidak ada perubahan Interface Contract (GD §0.4/§17.6) atau schema
Silver/Gold — layout partisi Bronze dan daftar internal `TIMEFRAMES` milik
MTF score keduanya adalah detail implementasi internal, bukan output yang
dijanjikan (schema kolom watchlist tidak berubah; hanya baris mana yang
lolos filter yang berubah). **1510 → 1521 test** (+11: 6 baru di
`test_market_ingester.py` untuk ADR-045/047, 1 test regresi + rewrite
fixture helper di `test_ohlcv_processor.py` untuk fix konsekuensial
Silver, +1 net di `test_mtf_alignment.py` setelah rework skema grade
ADR-046 Path C), **0 failed, 0 regresi**, coverage 88.04% (gate 80%). Lihat
`dev-log/2026-08-22-adr045-046-047-bronze-timeframe-mtf-coverage-au-ag-ticker.md`
untuk detail lengkap.

## v1.17.0 — Finnhub Full Retirement: Sentiment + Earnings/Quotes (Agustus 2026)

Diarahkan oleh `GMI_Decision_Document_v10.docx` (ADR-043, ADR-044) —
retirement penuh Finnhub sebagai data source. Sentiment
(`bronze_finnhub_sentiment`) mengembalikan `FinnhubAPIException(403)`
untuk seluruh 640 simbol universe pada live run `python src/runner.py
--job bronze` (21 Aug 2026) — plan-tier gate, bukan defect (401 untuk key
salah, bukan 403). Earnings/quotes (`bronze_finnhub`) tidak pernah live —
stub `NotImplementedError` sejak FIX R-F04, meski `FinnhubIngester`-nya
sendiri sudah schema-validated dan diuji (KNOWN_RISKS.md RISK-4, 40 test).
Ovi eksplisit memilih retirement penuh kedua-duanya sekaligus, bukan
verdict terpisah (sentiment dormant/accepted-risk, earnings/quotes saja
yang retired) — lihat ADR-043 Rationale untuk trade-off yang diterima.

Implementasi mengikuti checklist Section 3 dokumen tersebut secara
berurutan, ditambah **dua temuan baru** hasil grep sweep repo-wide wajib
pasca-retirement (bukan bagian checklist literal, tapi konsekuensi
mekanis langsung): `pipeline_scheduler.py` (GD §14.5, dormant APScheduler
path) akan `KeyError` saat diaktifkan karena masih menjadwalkan
`bronze_finnhub`/`silver_sentiment`; `SourceLimiters.finnhub`
(`rate_limiter.py`) sudah nol consumer sejak kedua ingester Finnhub
dihapus.

Total: **17 test dihapus** (4 file test dedicated + `TestCheckFinnhubShape`
di `test_preflight_scripts.py` + `TestSILAIO004FundamentalProcessor`/
`TestSILAIO004SentimentProcessor` di `test_preexisting_violations_v1.py`
+ `TestEnrichEarnings`/`TestEnrichSentiment` di `test_screener.py` + 6
parametrized `TestGlobalAuditClearance` case via 2 path removed dari 3
list), **9 test baru** (schema-stability assertion di `test_screener.py`;
3 replacement test di `test_job_registry_integrity.py`; 2 replacement
test di `test_runner_weekly_cadence.py`; `test_finnhub_limiter_removed`
di `test_rate_limiter.py`; 2 floor-assertion recalibration) | **1631 →
1510 passed / 0 failed / 0 error** (Δ −121, dikonfirmasi via 2x full
suite run berturut-turut, 0 skipped). MINOR bump (1.16.0 → 1.17.0):
perubahan struktural pada `JOB_REGISTRY` dan dependency graph
Bronze/Silver — sepadan skalanya dengan MINOR bump GMI-JR-003 sebelumnya
(kapabilitas baru tanpa perubahan schema); ini kebalikannya (penghapusan
kapabilitas), magnitude sama. Tidak ada perubahan Interface Contract (GD
§0.4, §17.6) atau schema Silver/Gold manapun.

### ARCHIVE ADR-043 [src/bronze/finnhub_ingester.py, src/bronze/finnhub_sentiment_ingester.py, src/silver/fundamental_processor.py, src/silver/sentiment_processor.py, config/schemas/finnhub_*.yaml (3 file), scripts/preflight/check_finnhub_shape.py] — Finnhub Bronze/Silver Modules Dipindah ke archive/, Dihapus dari Codebase Aktif

- ADR-043 awalnya menetapkan `git rm` langsung (bukan archive) —
  berbeda dari precedent RISK-1 (`tvdatafeed`, ADR-029) yang dipindah ke
  `scripts/archive/`. Rencana ini berubah di tengah implementasi karena
  alasan mekanis semata: sesi yang mem-mirror perubahan ke live repo
  punya akses read/write/move via Filesystem MCP connector tapi TIDAK
  punya kapabilitas delete file. Arahan Ovi: gunakan pendekatan archive
  sebagai gantinya. Seluruh 12 file dipindah byte-identical (diverifikasi
  via perbandingan ukuran terhadap git blob masing-masing; mtime asli
  dipertahankan) ke `archive/finnhub_retirement_2026_08/`, mencerminkan
  path aslinya persis, lengkap dengan README yang menjelaskan alasan
  archival dan memperingatkan agar tidak di-import, tidak di-collect
  sebagai test, dan tidak dijadikan restore path langsung. Secara
  fungsional ekuivalen dengan penghapusan — tidak ada satupun file ini
  yang di-import, dijadwalkan, atau reachable dari code path manapun
  yang live — bedanya hanya bytes-nya tetap ada di direktori archive,
  bukan cuma di git history.
- 4 file test dedicated ikut dipindah: `test_finnhub_ingester.py`,
  `test_finnhub_sentiment_ingester.py`, `test_fundamental_processor.py`,
  `test_sentiment_processor.py`.
- `tests/unit/test_preflight_scripts.py`: class `TestCheckFinnhubShape`
  dihapus (5 test) — file lain di test ini untuk preflight script lain
  tetap ada, tidak ikut terhapus. (File test ini sendiri TIDAK dipindah
  ke archive — hanya class di dalamnya yang dihapus, karena file ini
  masih menguji banyak preflight script lain yang tetap aktif.)

### FIX ADR-043 [src/scheduler/job_registry.py] — 4 Entry + 4 Wrapper Function Dihapus dari JOB_REGISTRY

- `_bronze_finnhub`, `_bronze_finnhub_sentiment`, `_silver_fundamental`,
  `_silver_sentiment` (wrapper function) dan 4 `JOB_REGISTRY` entry-nya
  dihapus total — bukan sekadar di-comment atau di-exclude dari sequence
  seperti FIX NEW-2 sebelumnya terhadap `silver_fundamental` saja.
- `bronze_finnhub_sentiment` + `silver_sentiment` dihapus dari
  `DAILY_SEQUENCE` (16 → 14 entry). Baris `silver_fundamental` yang
  sebelumnya di-comment-out di `WEEKLY_SEQUENCE` (menunggu Opsi B) juga
  dihapus — Opsi B kini closed, bukan deferred.
- `gold_screener.depends_on` tidak lagi mereferensikan
  `silver_sentiment` — supersedes guard FIX NEW-2 yang lebih sempit
  (hanya `silver_fundamental`). Comment block riwayat NEW-2/Opsi-B
  ditulis ulang untuk mencatat ADR-043/044 sebagai pengganti.
- `LAYER_JOB_NAMES` module comment diupdate: `bronze_finnhub` dan
  `silver_fundamental` sekarang tidak ada sama sekali di registry
  (bukan "terdaftar tapi sengaja di-exclude" seperti sebelumnya).
- Verifikasi import-level: 23 job tersisa di `JOB_REGISTRY` (dari 27),
  `DAILY_SEQUENCE` 14 entry, `gold_screener.depends_on ==
  ['gold_mtf', 'gold_regime', 'gold_sector']`.

### FIX ADR-044 [src/gold/screener.py] — _enrich_earnings()/_enrich_sentiment() Dihapus, Watchlist Schema Tidak Berubah

- `_enrich_earnings()` dan `_enrich_sentiment()` beserta kedua call site
  di `build_watchlist()` dihapus total — bukan dibiarkan meng-import
  modul yang sudah dihapus di dalam `except Exception` yang luas
  (persis anti-pattern RISK-13 yang sudah didokumentasikan sejarahnya
  di codebase ini).
- `days_to_earnings`, `next_earnings_date`, `near_earnings_flag`,
  `sentiment_score`, `buzz_score` tetap ada di skema output watchlist —
  sumbernya sudah selalu placeholder `NULL` bertipe eksplisit di main
  query `build_watchlist()`, bukan di kedua fungsi enrichment yang
  dihapus. Tidak ada perubahan Interface Contract (GD §0.4/§17.6):
  kolom yang dijanjikan tetap ada, tetap bertipe benar, sekadar
  permanently null, bukan sometimes-populated.
- Konstanta mati `SILVER_SENTIMENT`/`SILVER_SENTIMENT_ROOT` ikut
  dihapus — nol consumer tersisa setelah `_enrich_sentiment()` hilang.
- `tests/unit/test_screener.py`: `TestEnrichEarnings` (3 test) dan
  `TestEnrichSentiment` (4 test) dihapus; ditambahkan
  `test_watchlist_schema_stable_without_finnhub_enrichment` yang
  meng-assert langsung terhadap Parquet output tertulis — nama kolom,
  tipe (`Int32`/`Date`/`Boolean`/`Float64`/`Float64`), dan bahwa
  seluruh 5 kolom 100% null tanpa data Finnhub apapun.

### FIX ADR-043 [src/scheduler/pipeline_scheduler.py, src/utils/rate_limiter.py] — 2 Konsekuensi Tambahan dari Grep Sweep Repo-Wide

- `pipeline_scheduler.py` (GD §14.5, dormant APScheduler upgrade path,
  belum pernah diaktifkan): `_make_job()` melakukan `JOB_REGISTRY[name]`
  lookup langsung tanpa guard — akan `KeyError` pertama kali diaktifkan
  karena cron schedule masih menjadwalkan `bronze_finnhub` (02:30) dan
  `silver_sentiment` (03:30). Kedua entry dihapus dari docstring cron
  table dan dari `daily_schedule` list yang sesungguhnya.
- `SourceLimiters.finnhub` (`rate_limiter.py`): dikonfirmasi nol
  consumer di seluruh `src/` (grep sweep) — satu-satunya consumer yang
  mungkin, `finnhub_ingester.py`/`finnhub_sentiment_ingester.py`, sudah
  dihapus di fix yang sama. Dihapus sebagai konsekuensi mekanis
  langsung, bukan keputusan arsitektural terpisah.
  `tests/unit/test_rate_limiter.py`: `test_finnhub_limit_under_60`
  diganti `test_finnhub_limiter_removed` (assert `not hasattr(...)`).

### UPDATE [tests/, KNOWN_RISKS.md, pyproject.toml, .env.example, README.md] — Rekonsiliasi Test Suite, Dependency, dan Dokumentasi

- `tests/integration/test_job_registry_integrity.py`: 4 test stale
  (asumsi `silver_fundamental`/`silver_sentiment` masih terdaftar tapi
  di-exclude) diganti 3 test baru (assert tidak ada sama sekali di
  `JOB_REGISTRY`/kedua sequence/`gold_screener.depends_on`). 2 floor
  assertion (`len(DAILY_SEQUENCE) >= 15`) diturunkan ke `>= 13` dengan
  trace eksplisit ke ADR-043 — pengurangan yang disengaja, bukan
  regresi yang perlu disamarkan floor.
- `tests/integration/test_runner_weekly_cadence.py` (`GATE-N2`): ditulis
  ulang — `gold_screener` kini disatisfy langsung via 3 dependency
  aktualnya; test baru mengkonfirmasi `bronze_finnhub`/
  `silver_fundamental`/`silver_sentiment` gagal bersih via
  `SystemExit` yang sama seperti nama job tidak dikenal manapun.
- `tests/integration/test_full_system.py`: docstring
  `test_l7_pipeline_sequence_comprehensive` diupdate — Opsi B kini
  closed (retired), bukan "belum diimplementasikan"; floor `>= 13`
  tidak berubah nilainya, hanya rationale-nya.
- `tests/unit/test_preexisting_violations_v1.py`: 2 class dihapus
  (`TestSILAIO004FundamentalProcessor`, `TestSILAIO004SentimentProcessor`
  — 10 test) — target file sudah tidak ada. `TestGlobalAuditClearance`:
  2 entry `silver/fundamental_processor.py`/`silver/sentiment_processor.py`
  dihapus dari `AUDIT_SCOPE_FILES` dan dari parametrize
  `test_no_direct_write_parquet` — sebelumnya di-skip graceful via
  `exists()` guard (sumber "6 skipped" yang sempat muncul di satu run
  antara), kini dihapus total sebagai referensi basi, bukan dibiarkan
  skip selamanya.
- `KNOWN_RISKS.md`: RISK-4 judul + status diubah dari "✅ FIXED (dormant,
  hardened)" ke "✅ RESOLVED (retired)", struktur mengikuti persis RISK-1
  (`tvdatafeed`) — section historis dipertahankan sebagai catatan, section
  "Resolution (ADR-043 + ADR-044, 22 Aug 2026)" baru ditambahkan.
- `pyproject.toml`: `finnhub-python = "*"` dihapus dengan dated comment;
  `poetry.lock` diregenerasi (diff: persis 1 package dihapus, 0
  ditambahkan — `finnhub-python`); versi 1.16.0 → 1.17.0 dengan dated
  comment block mengikuti gaya GMI-JR-003/ADR-020/ADR-029 yang sudah ada.
- `.env.example`: `FINNHUB_API_KEY` dihapus total (bukan di-comment) —
  mengikuti convention file ini sendiri (direkonsiliasi terhadap
  `os.getenv()` call site nyata, dikonfirmasi nol via grep sweep), bukan
  precedent `TV_USERNAME`/`TV_PASSWORD` ADR-029 ("left as dead").
- `README.md`: "Data Sources (12)" → "(11)", baris Finnhub dihapus,
  dengan catatan pointer ke RISK-4; "Pipeline Jobs (27 registered)" →
  "(23 registered)"; `DAILY_SEQUENCE (16 jobs)` → "(14 jobs)" dengan
  listing job diupdate; project tree: `bronze/` 18→16 file,
  `silver/` 10→8 file, `config/schemas/` 12→10 YAML,
  `scripts/preflight/` — `check_finnhub_shape.py` dihapus dari listing;
  Layer Independence Guarantee table — baris Silver tidak lagi
  menyebut "one sanctioned supplement API call (Finnhub sentiment)";
  Environment Variables template — `FINNHUB_API_KEY` dihapus. Referensi
  historis (roadmap table "Solidification... Finnhub schema validation
  ✅ Complete (v1.9.0)") sengaja TIDAK diubah — catatan historis akurat
  pada waktunya, bukan klaim tentang state sekarang.

## v1.16.0 — Layer-Scoped Runner Commands: `--job bronze/silver/gold` (Agustus 2026)

Diarahkan langsung oleh Ovi ("add these commands to runner... during live
testing"), bukan berdasarkan GMI Decision Document tertulis. Menambahkan
tiga command aggregate baru ke `src/runner.py` yang menjalankan seluruh
job pada satu layer (Bronze, Silver, atau Gold) secara sequential, tanpa
perlu `--job all` — untuk mempermudah live testing satu layer pada satu
waktu.

Total: **18 test baru** (`tests/unit/test_runner.py` +7,
`tests/integration/test_job_registry_integrity.py` +9,
`tests/integration/test_runner_weekly_cadence.py` +3) | **1613 → 1631
passed/skipped / 0 failed / 0 error** | coverage aggregate tetap ~87.7%
(gate ≥80%, tidak turun). MINOR bump (1.15.3 → 1.16.0): kapabilitas baru,
backward-compatible sepenuhnya — tidak ada perubahan interface contract,
schema Silver/Gold, atau perilaku job existing manapun.

### ADD GMI-JR-003 [src/scheduler/job_registry.py, src/runner.py] — `--job bronze/silver/gold`

- **Apa:** `python src/runner.py --job bronze`, `--job silver`, dan
  `--job gold` masing-masing menjalankan seluruh job pada layer tersebut
  secara sequential, dalam urutan yang menghormati dependency chain.
- **Sumber job list:** `job_registry.layer_sequence(layer)` /
  `LAYER_JOB_NAMES` diturunkan dari `WEEKLY_SEQUENCE` (superset — weekly-
  only jobs + seluruh `DAILY_SEQUENCE`) alih-alih list terpisah yang
  di-maintain manual. Ini memastikan ketiga list tidak bisa drift dari
  `DAILY_SEQUENCE`/`WEEKLY_SEQUENCE` seiring job ditambah/dihapus/re-tag.
- **Exclusion otomatis:** `bronze_finnhub` (stub, FIX R-F04),
  `silver_fundamental` (orphaned, FIX NEW-2), dan 3 job manual-only
  (`bronze_bls_cpi`, `bronze_bls_nfp`, `bronze_bea_gdp` — sudah dicover
  `bronze_macro_weekly` via FRED mirror) otomatis tidak ikut karena memang
  tidak pernah masuk `DAILY_SEQUENCE`/`WEEKLY_SEQUENCE` — sama persis
  dengan scope `--job all` hari ini, tanpa list pengecualian kedua yang
  perlu di-maintain terpisah.
- **`health_report` (layer='util') sengaja tidak ikut** `--job gold` —
  scope command literal per-layer, bukan "gold + util cleanup".
- **Dependency check antar-layer TETAP berlaku tanpa `--force`:**
  `--job silver` akan `sys.exit(1)` jika bronze belum jalan hari ini;
  `--job gold` akan `sys.exit(1)` jika silver belum jalan. Ini disengaja —
  memverifikasi urutan Bronze → Silver → Gold yang benar (GD §17.2 Layer
  Independence Guarantee) selama staged live testing, bukan bug. Workflow
  yang dimaksud: `--job bronze` → `--job silver` → `--job gold`,
  berurutan, pada `run_date` yang sama.
- **Test baru:** integrity check `LAYER_JOB_NAMES` (semua job terdaftar
  di `JOB_REGISTRY`, layer field cocok, tidak ada duplikat, exclusion
  list terverifikasi, dedup branch di `layer_sequence()` diuji via
  `WEEKLY_SEQUENCE` sintetis); unit test `run_layer()` (layer tidak
  dikenal → `SystemExit(1)`, seluruh job bronze terpanggil sesuai urutan);
  integration test staged workflow penuh (bronze → silver → gold, tanpa
  `--force`, memakai `stubbed_registry`/`sandboxed_guard` fixture yang
  sudah ada).

## v1.15.3 — Coverage Tranche Phase 1–2: 25 Modul ke 100%, Zero Bug Produksi Ditemukan (Agustus 2026)

Diarahkan langsung oleh Ovi ("continue with the coverage tranche toward
95%"), bukan berdasarkan GMI Decision Document tertulis — kelanjutan
langsung dari precedent v1.12.1 Decision C coverage tranche (Juli 2026),
metodologi dan exclusion policy identik: `correlation_matrix.py` dan
`hmm_regime.py` tetap dikecualikan dari tranche ini per instruksi
eksplisit, tetap di denominator coverage. Target akhir 95% line coverage
aggregate; rilis ini menutup dua fase pertama dari lima fase yang
direncanakan.

Total: **120 test baru** (2 file baru: `test_base_ingester.py`,
`test_bea_ingester.py`; 15 file existing diperluas) | **1613 passed / 0
failed / 0 error** (Δ +120 dari v1.15.2 — 1493) | **coverage 81.46% →
88.36%** (830 → 521 baris belum ter-cover, Δ −309). PATCH bump (1.15.2 →
1.15.3): seluruh perubahan adalah penambahan test, tidak ada perubahan
interface contract, schema Silver/Gold, atau perilaku runtime apapun.

### Phase 1 — 17 file, 61 baris ditutup (81.46% → 82.82%)

`symbol_utils.py`, `eia_ingester.py`, `bls_ingester.py`,
`imf_ingester.py`, `schema_validator.py`, `base_ingester.py` (baru,
sebelumnya nol test sama sekali), `source_adapter.py`,
`dependency_guard.py`, `context_anchors.py`, `sentiment_processor.py`,
`global_rates_processor.py`, `mtf_alignment.py`, `ohlcv_aggregator.py`,
`views.py`, `atomic_io.py`, `progress_checkpoint.py`,
`pipeline_dashboard.py` — seluruhnya ke 100% line coverage (module-level
`if __name__ == "__main__":` guard dikecualikan via `pyproject.toml`'s
`exclude_lines`, konsisten dengan konvensi existing).

### Phase 2 — 8 file, 248 baris ditutup (82.82% → 88.36%)

`alphavantage_adapter.py`, `polygon_adapter.py`, `yfinance_adapter.py`,
`market_ingester.py`, `bea_ingester.py` (file test baru, melengkapi
`test_bea_ingester_gld001.py` yang sudah ada), `fred_ingester.py`,
`finnhub_ingester.py`, `bis_rates_ingester.py` — seluruhnya ke 100%.
Fokus fase ini: seluruh HTTP request/response body tiga adapter Bronze
utama (AlphaVantage, Polygon, yfinance) sebelumnya nol coverage — hanya
guard clause pre-request dan helper statis (mis. `_parse_pair()`) yang
teruji.

### Catatan Temuan — Gap Struktural, Bukan Bug Produksi

**`market_ingester.py` Layer 1 (`run()`/`_run_symbol()`/`_fetch()`) nol
test sejak awal.** Hanya jalur Layer 2 (context anchors, ditambahkan
belakangan pada GMI Wave 1 Cycle 3) yang punya test suite. Jalur trading
utama — Bronze OHLCV untuk 640 instrumen Layer 1 — berjalan di produksi
tanpa satupun test langsung selama ini. Tidak ada bug ditemukan pada
kode produksinya sendiri; gap murni pada test suite. 26 test baru
menutup `run()`, `_run_symbol()` (termasuk jalur ForexDayCache dan
cache-failure non-critical), `_fetch()` (empat varian ChainedAdapter per
market), dan dua cabang `_primary_source_for()` yang sebelumnya tidak
teruji (idx, forex).

**Test-isolation gap pada `SourceLimiters.alphavantage` (test
infrastructure, bukan `src/`).** `DailyBudgetLimiter` singleton tidak
pernah di-reset antar test dalam file yang sama —
`test_budget_exhausted_returns_none` (deliberately menghabiskan budget)
berpotensi meracuni test manapun yang berjalan setelahnya dalam sesi
yang sama, termasuk `test_unsupported_tf_returns_none` yang mengklaim
menguji cabang timeframe-tidak-didukung tapi bisa saja lolos karena
short-circuit budget-exhausted yang tidak disengaja, tergantung urutan
eksekusi pytest. Diperbaiki dengan fixture `autouse` yang me-reset
`_reset_date` sebelum/sesudah setiap test di `TestFetchHttpFlow`, plus
satu test baru (`test_unsupported_tf_returns_none_isolated`) yang secara
eksplisit mengkonfirmasi `requests.get` tidak pernah dipanggil untuk
cabang ini — membuktikan test lama PASS karena alasan yang benar, bukan
kebetulan. Kode produksi `alphavantage_adapter.py` sendiri tidak
berubah.

| File | Fase | Sebelum | Sesudah |
| --- | --- | --- | --- |
| `utils/symbol_utils.py` | 1 | 66% | 100% |
| `bronze/eia_ingester.py` | 1 | 95% | 100% |
| `bronze/bls_ingester.py` | 1 | 94% | 100% |
| `bronze/imf_ingester.py` | 1 | 95% | 100% |
| `bronze/schema_validator.py` | 1 | 96% | 100% |
| `bronze/base_ingester.py` | 1 | 0% (no tests) | 100% |
| `bronze/source_adapter.py` | 1 | 94% | 100% |
| `scheduler/dependency_guard.py` | 1 | 98% | 100% |
| `silver/context_anchors.py` | 1 | 87% | 100% |
| `silver/sentiment_processor.py` | 1 | 86% | 100% |
| `silver/global_rates_processor.py` | 1 | 93% | 100% |
| `gold/mtf_alignment.py` | 1 | 98% | 100% |
| `silver/ohlcv_aggregator.py` | 1 | 98% | 100% |
| `gold/views.py` | 1 | 98% | 100% |
| `utils/atomic_io.py` | 1 | 94% | 100% |
| `utils/progress_checkpoint.py` | 1 | 94% | 100% |
| `utils/pipeline_dashboard.py` | 1 | 99% | 100% |
| `bronze/alphavantage_adapter.py` | 2 | 47% | 100% |
| `bronze/polygon_adapter.py` | 2 | 45% | 100% |
| `bronze/yfinance_adapter.py` | 2 | 55% | 100% |
| `bronze/market_ingester.py` | 2 | 59% | 100% |
| `bronze/bea_ingester.py` | 2 | 59% | 100% |
| `bronze/fred_ingester.py` | 2 | 87% | 100% |
| `bronze/finnhub_ingester.py` | 2 | 82% | 100% |
| `bronze/bis_rates_ingester.py` | 2 | 84% | 100% |

Sisa gap menuju target 95%: **297 baris** across Silver
(`quality_validator.py` — 110 baris, terbesar tunggal), Gold
(`macro_regime.py`, `technical_signals.py`, dst. —
`correlation_matrix.py` dan `hmm_regime.py` dikecualikan), dan
orchestration (`job_registry.py`, `runner.py`, dst.) — Fase 3–5, belum
dikerjakan.

## v1.15.2 — Data Source Preflight Remediation: EIA APIv2, BEA Table/Line Corrections, FRED Registry Hygiene (Agustus 2026)

Dokumen referensi: GMI_Decision_Document_v9.docx (14 Aug 2026).

Total: **5 ADR diimplementasikan (ADR-038, ADR-039, ADR-040, ADR-041,
ADR-042)** | **8 file dimodifikasi (2 config, 4 source, 3 preflight
script — 1 preflight script dihitung dua kali karena menyentuh 2 ADR) +
3 file test (1 baru, 2 diperbarui)** | **1492 collected / 0 failed / 0
error** (baseline v1.15.1: 1466).

Implementasi mengikuti hasil live run 14 Aug 2026 dari 8 preflight
script yang ditulis thread yang sama (`check_fred_series.py`,
`check_eia_series.py`, `check_bea_datasets.py`, dst. — lihat
RISK-17/18/19, `2026-08-14-alpha-factory_preflight_logs.txt`). Ketiga
preflight script yang disentuh release ini (`check_fred_series.py`,
`check_eia_series.py`, `check_bea_datasets.py`) ada hanya di local
working directory Ovi (belum pernah di-commit ke GitHub) — dibawa masuk
ke sandbox clone terisolasi via isi file yang dibaca langsung dari live
filesystem sebelum divalidasi. Semua perubahan divalidasi di sandbox
clone terisolasi (`ast.parse()`, full `pytest`, coverage aggregate) sebelum
di-mirror ke live repo via Filesystem MCP dengan read-back verification.

### ADR-038 [HIGH] — `src/bronze/eia_ingester.py`, `scripts/preflight/check_eia_series.py`, `config/schemas/eia_oil.yaml`
**EIA APIv1 confirmed fully dead (discontinued Nov 2022) — setiap run `bronze_eia` mingguan sejak deploy menulis nol baris secara diam-diam selama ~3.75 tahun**

- Root cause: `check_eia_series.py` live run pertama (14 Aug 2026) —
  4/4 series FAIL, HTTP 404, baik batch maupun isolated. EIA's own
  documentation mengkonfirmasi APIv1 didiskontinuasi penuh November
  2022. `FIX EIA-1`'s rationale asli ("v1 stabil, v2 category paths
  bervariasi per dataset") sudah void untuk route spesifik ini.
- Opsi yang dipertimbangkan: full v2 route/facet redesign (per-series
  category path research) — DITOLAK, surface area lebih besar dan
  tidak perlu ketika `/v2/seriesid/{id}` backward-compat route sudah
  menyelesaikan breakage dengan risiko jauh lebih kecil.
- Fix: migrasi ke APIv2 `/v2/seriesid/{id}` — menerima legacy v1-style
  series ID (`PET.WCRSTUS1.W` dst.) langsung tanpa remap penuh ke
  category/facet tree v2. Response parsing diupdate dari v1's
  `data.series[0].data = [[period, value], ...]` ke v2's
  `response.data = [{"period": ..., "value": ..., ...}, ...]`.
  `config/schemas/eia_oil.yaml`'s `expected_columns` di-reverifikasi
  (checklist item 5) — tidak ada perubahan field diperlukan, karena
  `_fetch_series()` menormalisasi kedua shape ke internal record yang
  identik sebelum schema validator dikonsultasi.
- Diverifikasi empiris: smoke test manual dengan mock v2-shaped response
  mengkonfirmasi URL (`https://api.eia.gov/v2/seriesid/{series_id}`),
  params, dan parsing logic benar. **Belum** dikonfirmasi terhadap live
  response sesungguhnya — sandbox ini tidak punya network route ke
  `api.eia.gov` — pending live confirmation (checklist item 4).
- Test baru/diperbarui: `tests/unit/test_eia_ingester.py` ditulis ulang
  penuh untuk v2 response shape (16 test, semua lulus). Dua test
  di-rename untuk mencerminkan kontrak baru
  (`test_no_key_still_attempts_v2_request`,
  `test_no_response_envelope_no_write`) — perilaku lama yang diuji
  sudah dihapus, sesuai konvensi test-update-saat-kontrak-berubah
  (CI/CD Ops Guide anti-pattern table).

### ADR-039 [P2] — `src/bronze/bea_ingester.py`, `scripts/preflight/check_bea_datasets.py`
**BEA `pce_deflator` LineDescription match gagal 0/310 baris live — diganti LineNumber-based matching**

- Root cause: `check_bea_datasets.py` live run pertama (14 Aug 2026) —
  `pce_deflator` (T20304) mengembalikan 310 baris, nol yang cocok
  dengan `LINE_FILTER["pce_deflator"]`'s exact-match string. Pemilihan
  tabel T20304 sendiri dikonfirmasi benar via BEA's own NIPA table
  register — hanya string match yang salah.
- Fix: `LINE_NUMBER_FILTER["pce_deflator"] = "1"` — BEA NIPA table
  convention menempatkan baris headline/aggregate di `LineNumber` tetap,
  struktural terhadap format tabel, bukan label yang BEA ubah
  sembarangan. `LINE_FILTER`'s string dipertahankan sebagai
  human-readable label saja untuk `pce_deflator`. `real_gdp` TIDAK
  diubah — masih cocok live via `LineDescription`.
- Diverifikasi empiris: smoke test dengan row yang punya
  `LineDescription` BERBEDA dari string lama tapi `LineNumber="1"` yang
  benar — matching berhasil, membuktikan fix robust terhadap wording
  drift yang sama persis yang mematahkan match lama.
- Test baru/diperbarui: `tests/unit/test_bea_ingester_gld001.py`'s
  `test_pce_deflator_filters_correct_row` ditulis ulang dengan wording
  sengaja berbeda pada baris target. Test baru
  `test_line_number_filter_adr039_040`.

### ADR-040 [P2, pending live confirmation] — `src/bronze/bea_ingester.py`, `scripts/preflight/check_bea_datasets.py`
**BEA `trade_balance` membaca tabel yang salah sama sekali — T40100 (current account) bukan GDP net exports**

- Root cause: T40100 dikonfirmasi, via baris yang dikembalikannya
  sendiri ("Balance on current account, NIPAs", "Current payments to
  the rest of the world", ...), sebagai tabel International
  Transactions/current-account (balance-of-payments) BEA — konsep
  berbeda dan lebih luas dari komponen GDP "net exports of goods and
  services" (current account juga menetkan primary/secondary income
  flows yang GDP accounting kecualikan). Ini terbaca sebagai tabel
  salah sejak desain awal, bukan wording drift.
- Opsi yang dipertimbangkan: pertahankan T40100 dan cari baris
  current-account yang mendekati "net exports" — DITOLAK, current
  account dan net exports of goods/services bukan angka yang sama;
  akan menghasilkan series yang plausible-looking tapi secara konsep
  salah, lebih buruk dari series kosong yang ada sekarang.
- Fix: `table_name` diganti `T40100` → `T10105` (Table 1.1.5, Gross
  Domestic Product — tabel standar komponen GDP), matching via
  `LineNumber == "15"`. LineNumber diinferensikan dari struktur baris
  standar Table 1.1.x (dikonfirmasi via listing Table 1.1.3 riil yang
  berbagi layout baris identik — Line 15 = "Net exports of goods and
  services").
- **Belum diselesaikan**: LineNumber persis untuk T10105 belum
  dikonfirmasi empiris terhadap live response pipeline ini sendiri —
  hanya diinferensikan dari referensi eksternal + tabel sibling dengan
  layout sama. Perlakukan sebagai decided-in-direction, bukan
  decided-in-detail, sampai live run (checklist item 10) terjadi.
- Test baru/diperbarui: `test_trade_balance_filters_correct_row`
  ditulis ulang dengan wording sengaja berbeda. Test baru
  `test_trade_balance_table_switched_to_t10105`.

### ADR-041 [P2] — `config/fred_series.yaml`, `src/bronze/fred_ingester.py`, `src/bronze/bls_ingester.py`, `scripts/preflight/check_fred_series.py`
**5 FRED series mati/redundan di-prune — plus grep-sweep cleanup 2 referensi mati yang ditemukan**

- Root cause: `check_fred_series.py` live run pertama (14 Aug 2026) —
  check live pertama terhadap 61 series non-commodity di
  `config/fred_series.yaml` (RISK-15's `check_fred_commodity_series.py`
  hanya pernah cover 6 series `domain: commodity`). 3 hard failure
  (HTTP 400): `GOLDAMGBD228NLBM` (dihapus FRED 2022-01-31), `NAPM` &
  `NMFCI` (dihapus FRED 2016-06-24, tanpa pengganti gratis). 2 series
  frozen: `PPIFGS` (didiskontinuasi BLS ~Feb 2016, redundan dengan
  `PPIFIS`), `CSCICP03USM665S` (frozen sejak 2024-01, redundan dengan
  `UMCSENT`).
- Fix: 5 series dihapus dari `config/fred_series.yaml` (67 → 62). Tidak
  ada yang berada di `regime_inputs` — deteksi macro regime tidak
  terdampak. Grep-sweep (checklist item 11) menemukan dan membersihkan
  2 referensi mati sesungguhnya: `fred_ingester.py`'s `RELEASE_LAG_DAYS`
  (5 key inert dihapus) dan `bls_ingester.py`'s
  `fred_mirror_map["PPI"]` (`PPIFGS` dihapus, `PPIFIS` dipertahankan).
- Test baru/diperbarui: `tests/unit/test_fred_series_registry_adr041_042.py`
  (file baru, 24 test) — mencakup keduanya ADR-041 dan ADR-042 (lihat
  bawah), termasuk `TestGrepSweepCleanup`.

### ADR-042 [P2] — `config/fred_series.yaml`
**6 tenor Treasury yang selalu dideklarasikan tapi tak pernah teregistrasi — silently dropped oleh series_filter's registry-gate mechanism**

- Root cause: `treasury_ingester.py`'s `TREASURY_FRED_SERIES` selalu
  mendeklarasikan 13 tenor (1M s/d 30Y), tapi
  `FREDIngester.run()`'s `series_filter` hanya bisa mempertahankan
  series yang sudah ada di registry yang di-load — `DGS1MO`, `DGS3MO`,
  `DGS6MO`, `DGS1`, `DGS7`, `DGS20` tidak pernah teregistrasi, sehingga
  silently dropped setiap run meski disebut namanya di filter. Bronze
  hanya pernah meng-ingest 4 dari 13 tenor yang dideklarasikan
  (2Y/5Y/10Y/30Y) — closes gap antara GD v1.2 §3.3.3's "full 1M–30Y
  yield curve" dan realita ingestion.
- Fix: 6 tenor didaftarkan di bawah domain `monetary_policy`, mengikuti
  pola entry `DGS2`/`DGS5`/`DGS10`/`DGS30` yang sudah ada (62 → 68
  total). **Sesuai keputusan ADR-042 sendiri: TIDAK ada perubahan kode
  di `treasury_ingester.py` maupun `fred_ingester.py`** — 6 tenor baru
  ini sengaja TIDAK mendapat entry `RELEASE_LAG_DAYS` (fallback ke
  default 7 hari, bukan 1 hari seperti sibling-nya) — keputusan scope
  eksplisit, bukan oversight.
- Diverifikasi empiris: `TestSilentDropMechanismFixed` mereproduksi
  mekanisme bug persis yang dideskripsikan ADR menggunakan file
  registry PRODUKSI SESUNGGUHNYA (bukan mock) —
  `series_filter=["DGS20"]` terhadap `FREDIngester()` sekarang benar-
  benar mencapai `fredapi.Fred.get_series()` dan menulis file Bronze,
  di mana sebelum fix ini akan silently mempertahankan 0 series dan
  menulis apa-apa, tanpa error.

### Catatan cakupan — item yang DITEMUKAN tapi SENGAJA TIDAK diubah
- ADR-040's exact `LineNumber` untuk T10105 — diinferensikan, belum
  dikonfirmasi live untuk parameter request pipeline ini sendiri.
  Genuinely open, di-flag bukan di-default (checklist item 10,
  `GMI_Decision_Document_v9.docx` §4).
- ADR-038's v2 response shape — dikonfirmasi via dokumentasi resmi EIA
  + contoh response yang dipublikasikan, TIDAK dikonfirmasi terhadap
  live response 4 series spesifik ini (checklist item 4).
- Grep sweep item 11 (ADR-041) diselesaikan penuh thread ini — 2
  referensi mati ditemukan dan dibersihkan (`RELEASE_LAG_DAYS`,
  `bls_ingester.py`'s FRED-mirror map).
- `bea_ingester.py`'s `run()` dan `_run_via_fred_mirror()` method-level
  coverage tetap di bawah 80% terisolasi (56-59%) — pre-existing gap
  tidak disentuh ADR-039/040 manapun (kedua ADR hanya mengubah
  `_fetch_nipa()`'s matching logic, yang coverage-nya utuh via test
  yang ada). Aggregate repo-wide coverage tetap 81%+, di atas gate G-6.

### Version bump rationale
PATCH (1.15.1 → 1.15.2), bukan MINOR: seluruh perubahan adalah bug fix
terhadap ingester yang sudah ada (EIA/BEA silently broken) dan config
pruning/addition (FRED registry hygiene) — bukan job/market/indicator
baru yang diekspos ke downstream consumer manapun (preseden v1.13.4/
v1.13.5, bukan v1.14.0/v1.15.0's MINOR precedent untuk kapabilitas
baru).

---

## v1.15.1 — Taxonomy Hygiene & Proxy Correlation Discipline (Agustus 2026)

Dokumen referensi: GMI_Decision_Document_v8.docx (10 Aug 2026).

Total: **4 ADR diimplementasikan (ADR-034, ADR-035, ADR-036, ADR-037)** |
**8 file dimodifikasi (2 config, 2 schema, 2 source, 1 script) + 6 file test** |
**1466 collected / 0 failed / 0 error** (baseline v1.15.0: 1460).

Implementasi mengikuti hasil live run 10 Aug 2026 dari `check_proxy_correlation.py`
dan `check_bis_eer_weights.py --extract-weights` (lihat RISK-1/RISK-16,
`2026-08-10-alpha-factory preflight logs.txt`). Divalidasi di sandbox clone
terisolasi (`validate_instruments.py` exit 0, `pytest` penuh) sebelum
di-mirror ke live repo via Filesystem MCP.

### ADR-034 [P2] — `config/instruments_taxonomy.yaml`
**Proxy deferral dibedakan per kekuatan korelasi, bukan diseragamkan**

- Root cause: `check_proxy_correlation.py` dijalankan live untuk pertama
  kali — NICKEL +0.586/36bln, CPO +0.405/120bln, RUBBER +0.229/120bln,
  TIN +0.139/120bln — seluruhnya jauh di bawah preseden platform sendiri
  (VALE ~0.81 ADR-005, WHC.AX ~0.78 ADR-006).
- Opsi yang dipertimbangkan: hapus total keempatnya (ditolak — akan
  mengosongkan `context_commodity_agri` dan gagal validator coverage 22
  subkategori); perlakuan seragam (ditolak — NICKEL 0.586 tidak sebanding
  dengan TIN 0.139).
- Fix: TIN dan RUBBER kembali `context_available: false` (`deferred_reason`
  + `planned_wave: 2` diisi sebagai placeholder struktural — bukan
  reasersi trigger FX-normalization lama). CPO dan NICKEL tetap aktif,
  `proxy_for`/`proxy_correlation_expected` diisi nilai terukur dengan
  caveat "moderate, not strong" di `notes`.
- Diverifikasi empiris: `validate_instruments.py` — 699 symbols, Layer
  1=639, Layer 2=60, `deferred_count()==2`.
- Test baru/diperbarui: `test_deferred_instruments_are_tin_and_rubber`,
  `test_forecast_context_cpo_nickel_active_tin_rubber_deferred`,
  `test_resolve_excludes_tin_and_rubber_includes_cpo_and_nickel`, dan
  count assertions di seluruh `test_instrument_loader.py`/
  `test_context_anchors.py`/`test_full_system.py`.

### ADR-035 [P3] — `config/instruments_{identity,taxonomy}.yaml`, `scripts/validate_instruments.py`, `config/schemas/instruments/*.schema.yaml`
**Kategori market `index: []` yang vestigial dihapus**

- Root cause: `index` kosong di kedua file YAML sejak ADR-003 (SPX/VIX
  direklasifikasi ke Layer 2). `instrument_loader.py`'s own comment sudah
  mendokumentasikan ini permanen; `silver_scope.py::layer1_markets()`
  tidak pernah menyertakannya (derivasi dinamis).
- **Temuan tambahan saat implementasi (di luar decide-phase awal
  ADR-035):** kedua file `config/schemas/instruments/*.schema.yaml`
  mendeklarasikan `index` sebagai `required` top-level property —
  tidak diperiksa saat decide-phase ADR-035 sendiri. Menghapus key
  `index` dari YAML tanpa memperbaiki schema akan membuat
  `validate_split()` gagal (`jsonschema` required-property error).
  Diperbaiki sebagai consequential fix bercakupan sama dengan ADR-035
  (kategori debt yang sama persis yang ADR ini targetkan).
- Fix: `index: []` dihapus dari kedua file config; `"index"` dihapus dari
  `REQUIRED_FIELDS` dan tuple `layer1_markets` di
  `validate_instruments.py`; `index` dihapus dari `required` +
  `properties` di kedua schema file.
- Diverifikasi empiris: `validate_instruments.py` exit 0 pasca-perubahan;
  `jsonschema.validate()` langsung terhadap dict minimal tanpa key
  `index` — lolos.
- Test baru: `test_index_key_absent_from_real_files`,
  `test_index_not_in_required_fields_or_layer1_markets`,
  `test_index_not_required_by_schema`,
  `test_split_file_without_index_key_still_validates`.

### ADR-036 [P2] — `config/instruments_{identity,taxonomy}.yaml`
**USD_IDR direklasifikasi: Layer 1 forex → Layer 2 `context.dollar_basket`**

- Root cause: 13 target-currency legs Broad Dollar Index terpecah dua
  jalur sourcing (7 via Layer 1 forex termasuk USD_IDR, 6 via
  `dollar_basket`) — desain `dollar_basket`'s sendiri hanya menjelaskan
  6 yang terakhir. USD/IDR sebagai pair trading standalone redundan
  dengan exposure Indonesia yang sudah ada (30 saham IDX30 + BI rate
  context anchor, sudah 2x-weighted di `score_em_risk` per Data Source
  & Rates Adjustment v1.0 §7.2).
- Fix: `USD_IDR` dipindah keluar dari Layer 1 forex, masuk
  `context.dollar_basket` sebagai `IDR` (mengikuti konvensi
  bare-currency-code 6 anggota lain), `raw_symbol` dihapus,
  `reclassified_from: layer_1_forex` ditambahkan (pola audit-trail yang
  sama dengan DXY/SPX/VIX, ADR-003).
- Konsekuensi eksplisit: USD/IDR keluar dari eligibility
  `gold_signals`/`gold_mtf`/`gold_screener` — trade-off yang dikonfirmasi
  disengaja, bukan efek samping. Layer 1 forex: 19 → 18. `dollar_basket`:
  6 → 7. EXPECTED_TOTAL tetap 699 (reklasifikasi lintas-layer, bukan
  penambahan — preseden ADR-003).
- Diverifikasi empiris: `validate_instruments.py` — Layer 1=639
  (was 640), Layer 2=60 (was 59).
- Test baru: `test_idr_reclassified_from_layer1_forex`,
  `test_dollar_basket_subcategory_has_seven_currencies`.

### ADR-037 [P3] — `config/instruments_{identity,taxonomy}.yaml`
**`context.fx_normalization`: MYR dihapus, THB ditambahkan**

- Root cause: MYR (ADR-024) ada untuk satu tujuan — normalisasi CPO's
  raw FCPO Bursa Malaysia feed. Orphaned sejak ADR-030 me-resource CPO
  ke F34.SI (SGX, SGD-denominated). Keempat proxy equity saat ini
  (F34.SI/SGD, STA.BK/THB, AFM.V/CAD, NIC.AX/AUD) semuanya
  local-currency-denominated, bukan USD.
- Fix: entry MYR dihapus, entry THB ditambahkan (kebutuhan normalisasi
  STA.BK/RUBBER di masa depan — RUBBER sendiri deferred lagi per
  ADR-034 dalam dokumen yang sama). `_meta.note` ditambahkan menjelaskan
  AUD/CAD/SGD sengaja TIDAK diduplikasi di sini — sudah tersedia via
  Layer 1 forex (`AUD_USD`, `USD_CAD`) atau `context.dollar_basket`
  (`SGD`, ADR-016); `compute_broad_dollar()` (Architecture v2.0 §7.2)
  sudah membaca Layer 1 forex langsung by name, pola reuse-dari-sumber
  yang sama harus diikuti konsumen normalisasi FX di masa depan.
- Diverifikasi empiris: `requires_fx_normalization`/`base_currency`
  dikonfirmasi tidak dibaca di manapun dalam `src/` (bukan typed
  `InstrumentLoader` field, catch-all `meta` dict saja) — penghapusan
  MYR adalah debt removal bersih, tanpa consumer yang terputus.
- Test baru/diperbarui: `test_fx_normalization_subcategory_has_thb_only`,
  `test_thb_excluded_from_forecast`,
  `test_thb_ticker_matches_bare_currency_convention`,
  `test_fx_normalization_does_not_duplicate_aud_cad_sgd`.

### Catatan cakupan — item yang DITEMUKAN tapi SENGAJA TIDAK diubah
- `src/utils/symbol_utils.py::KNOWN_EDGE_CASES["MYR"]` — dict dokumentasi
  murni (own comment: "NOT consulted at runtime"), sekarang stale, tapi
  tidak disebut di scope decided ADR-037 manapun. Deeper dead-code sweep
  eksplisit di luar-scope per pola ADR-035 sendiri.
- `scripts/preflight/check_yfinance_tickers.py` referensi `MYR` (manual
  ticker-check tool) — di luar scope 4 ADR ini.
- Gate 1 (BIS Broad Dollar weight) persistence mechanism — genuinely
  open per `GMI_Decision_Document_v8.docx` §4, tidak diselesaikan di
  release ini.

### Version bump rationale
PATCH (1.15.0 → 1.15.1), bukan MINOR: seluruh perubahan adalah koreksi
taksonomi/data quality dan reklasifikasi (preseden ADR-003), bukan job/
market/indicator baru. `GMI_Decision_Document_v8.docx` §3 item 12
menyerahkan pilihan PATCH vs MINOR eksplisit ke implementer.

---

## v1.15.0 — RISK-15 Live-Confirm, Gate 1 Extraction Pass, Studi Korelasi Proxy (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — sesi
tiga-bagian yang tidak menyentuh `src/`, menutup verifikasi live RISK-15
yang tersisa dan menulis dua kemampuan preflight baru: Gate 1
(ADR-017/018, RISK-16) dan studi korelasi proxy (RISK-1 residual gap).

Total: **1 verifikasi live ditutup** (RISK-15) | **2 preflight script
baru/diperluas** (`check_bis_eer_weights.py --extract-weights`,
`check_proxy_correlation.py`) | **28 test baru** | **1432 → 1460
passed / 0 failed / 0 error**.

### VERIFY Risk15LiveFred [KNOWN_RISKS.md] — `check_fred_commodity_series.py` Dijalankan Live di M1, Semua 6 Series PASS

**Root cause**: v1.14.0 menutup RISK-15 secara kode/config, tapi
entry-nya sendiri secara eksplisit menyisakan "Not yet run: a real
FRED_API_KEY-backed invocation ... against live FRED" sebagai catatan
terbuka.

**Fix**: Ovi menjalankan `check_fred_commodity_series.py` di M1 dengan
`FRED_API_KEY` asli — seluruh 6 series PASS (`PIORECRUSDM`,
`PCOALAUUSDM`, `PPOILUSDM`, `PRUBBUSDM`, `PTINUSDM`, `PNICKUSDM`,
semuanya `latest=2026-06-01`, 12/12 observasi usable). KNOWN_RISKS.md
RISK-15 diupdate untuk mencatat ini — separuh catatan "Not yet run"
ditutup.

**Yang belum**: full `poetry run pytest` di hardware nyata
mengkonfirmasi 1432/1432 secara empiris — tidak termasuk dalam run 9
Agustus ini, dan belum dikonfirmasi terpisah.

### ADD Gate1ExtractionPass [scripts/preflight/check_bis_eer_weights.py, tests/unit/test_preflight_scripts.py] — `extract_us_weights_from_sheet()` + `--extract-weights` CLI Mode

**Root cause**: KNOWN_RISKS.md RISK-16/Gate 1 section (4 Agustus 2026)
mencatat layout `weightsb.xlsx` sudah sepenuhnya dikarakterisasi tapi
nilai per-currency belum diekstrak — scan `--discover-weights`
menemukan posisi 13 target currency sebagai KOLOM header, tapi tidak
pernah mencari baris US sendiri (US bukan salah satu dari 13 target
REF_AREA).

**Fix**: `extract_us_weights_from_sheet()` (pure function, tanpa I/O)
ditambahkan — mencari baris dengan kolom-2 == "US", mencari baris
header secara independen (>=2 match dari 13 target REF_AREA sebagai
penanda, bukan hardcode row 6), lalu membaca intersection: nilai baris
US di setiap kolom target currency. Posisi row/column DIDERIVASI ULANG
setiap kali dipanggil — tidak mempercayai temuan "identik di seluruh 10
sheet" dari run 4 Agustus sebagai layout tetap, mengikuti disiplin
"jangan tebak layout, verifikasi" yang sama yang sudah menemukan &
memperbaiki 3 kesalahan tebakan sebelumnya di script ini (WS_CBPOL_D,
WS_EER_M, segmen "structure/" yang hilang). `--extract-weights` CLI
mode ditambahkan, default ke vintage sheet terbaru (`max(sheetnames)` →
`2020_2022`), dengan override `--sheet` dan `--us-ref-area`.

**Diverifikasi (empiris, sandbox saja)**: 11 test baru
(`TestCheckBisEerWeights::test_extract_*`) terhadap workbook sintetis
yang meniru layout nyata yang sudah dikonfirmasi (run 4 Agustus) —
happy path 13 currency, US row hilang → None, currency column hilang →
partial result (bukan total failure), override `us_ref_area`,
auto-select vintage terbaru, override `--sheet` manual, unknown sheet →
fail bersih, US row tidak ditemukan → fail bersih, CLI wiring. Satu
kesalahan penulisan test ditemukan dan diperbaiki saat proses ini
sendiri (assertion awal salah mengecek nilai baris Australia, bukan
baris US, yang dibangun test itu sendiri) — dijaga di komentar test
sebagai jejak audit. **Gate 1 belum ditutup** — ini baru menulis &
menguji-unit kode ekstraksi, bukan menjalankannya terhadap file
bis.org yang sesungguhnya (tidak ada sandbox di proyek ini yang punya
rute jaringan ke bis.org). Menjalankan `--extract-weights` di M1
adalah langkah berikutnya.

### ADD ProxyCorrelationStudy [scripts/preflight/check_proxy_correlation.py, tests/unit/test_preflight_scripts.py] — Studi Korelasi CPO/RUBBER/TIN/NICKEL vs FRED Track 2

**Root cause**: `instruments_taxonomy.yaml` sendiri mencatat inline di
keempat entry NICKEL/TIN/CPO/RUBBER bahwa `proxy_for`/
`proxy_correlation_expected` sengaja tidak diisi — belum ada studi
korelasi empiris (berbeda dari VALE yang punya ~0.81, ADR-005).
`validate_instruments.py` mewajibkan `proxy_correlation_expected`
setiap kali `proxy_for` ada, jadi keempat instrumen ini tidak bisa
mendapat salah satu field sampai angka nyata ada. RISK-15 sendiri
(v1.14.0) mencatat: "the correlation studies need a commodity price
benchmark ... these 6 series are exactly that benchmark, now
available" — benchmark itu baru dikonfirmasi live hari ini (lihat
entry VERIFY di atas).

**Fix**: `scripts/preflight/check_proxy_correlation.py` baru. Karena
tidak ada benchmark harian resmi untuk keempat komoditas ini (persis
kenapa mereka di-proxy sejak awal — lihat ADR-029), metodologinya
berbeda dari VALE/Iron Ore ("rolling 60D" terhadap benchmark harian):
proxy yfinance di-resample ke closing bulanan (tanggal 1 tiap bulan,
selaras dengan konvensi tanggal FRED), dikorelasikan sebagai
month-over-month return (bukan level harga — menghindari spurious
correlation dua seri yang sama-sama trending, mengikuti prinsip yang
sama dengan CorrelationModule platform ini, Architecture v2.0 §6.2)
terhadap return bulanan FRED Track 2. `compute_proxy_correlation()`
(pure function, pakai `statistics.correlation` stdlib) dipisah dari
I/O — pola separation-of-concerns yang sama dengan
`extract_us_weights_from_sheet()`. Minimum 12 pasang return overlap
diwajibkan (fail closed, bukan korelasi dari sedikit titik data yang
menyesatkan — sikap yang sama dengan SchemaValidator's quarantine, GD
§3.7).

**Diverifikasi (empiris, sandbox saja)**: 17 test baru
(`TestCheckProxyCorrelation`) — matematika korelasi murni (korelasi
positif sempurna, negatif sempurna, overlap tidak cukup → None, varians
nol → None tanpa crash), penanganan error I/O wrapper (yfinance
exception, data proxy kosong, FRED exception, data FRED kosong), pesan
sukses mengutip titik referensi VALE/ADR-005 (bukan threshold
pass/fail yang dikarang), CLI wiring. Cross-consistency guard
ditambahkan: `BENCHMARK_SERIES` di script ini divalidasi sebagai subset
dari `check_fred_commodity_series.py`'s `EXPECTED_COMMODITY_SERIES`.
Seluruh 71 test di `test_preflight_scripts.py` (43 lama + 28 baru)
dikumpulkan dan lolos bersama di sandbox terisolasi sebelum kedua
script menyentuh repo asli. **Belum dijalankan** terhadap
yfinance/FRED nyata — hasil korelasi aktual, dan keputusan mengisi
`proxy_for`/`proxy_correlation_expected` di `instruments_taxonomy.yaml`,
adalah langkah tindak lanjut yang disengaja tidak dilakukan script ini.

**Test baseline**: `tests/COUNT_BASELINE.txt` 1432 → 1460 (+28).

---

## v1.14.0 — RISK-15 Diselesaikan: FRED Track 2 Commodity Supplements (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — thread baru,
menutup KNOWN_RISKS.md RISK-15 yang diflag 30 Juli 2026 (thread
ADR-030–033, GMI_Decision_Document_v7.docx) sebagai "flagged, not
fixed."

Total: **1 gap konfigurasi ditutup** (commodity domain baru di
fred_series.yaml, 6 series) | **1 bug dokumentasi ditemukan dan
diperbaiki** (Iron Ore series ID salah di 2 dokumen sebelumnya) |
**1 script preflight baru** | **1422 → 1432 passed / 0 failed / 0
error**.

### FIX FredCommodityDomain [config/fred_series.yaml, src/bronze/fred_ingester.py, KNOWN_RISKS.md] — Commodity Domain Ditambahkan, 6 Series (2 Lama + 4 Baru)

**Root cause**: `config/fred_series.yaml` tidak punya domain
`commodity` sama sekali. Architecture Extension v1.0 ADR-005/006
sudah men-decide 2 series (`PIORECRORECUSDM` untuk Iron Ore,
`PCOALAUUSDM` untuk Coal Australia) sebagai FRED Track 2 supplement
untuk proxy VALE/WHC.AX — tidak pernah benar-benar ditambahkan ke
file. Thread ADR-030–033 (30 Juli 2026) menambahkan 4 kandidat baru
dengan pola sama (`PPOILUSDM`, `PRUBBUSDM`, `PTINUSDM`, `PNICKUSDM`
untuk CPO/RUBBER/TIN/NICKEL) tapi juga sengaja tidak menambahkannya —
di luar scope thread tvdatafeed-retirement itu, dan belum
terverifikasi live.

**Fix**: Section `commodity` baru ditambahkan dengan 6 series, seluruh
`regime_input: false` (bukan macro regime input — Track 2 adalah
input masa depan untuk `ForecastModule`, GMI Wave 1 Cycle 4 /
CrossAssetEngine belum dibangun). `src/bronze/fred_ingester.py`
mendapat entry `RELEASE_LAG_DAYS` untuk keenam series (25 hari,
mengikuti presedan series PPI yang sudah ada dan lag publikasi ~24
hari yang diobservasi langsung di fred.stlouisfed.org). Komentar
header stale ("60 series") sekalian diperbaiki ke 67 — sudah selisih
1 (VIXCLS) sebelum perubahan ini.

**Diverifikasi**: `fred_ingester.py` dan `macro_processor.py` dibaca
langsung — keduanya domain-agnostic (domain hanya dipakai untuk path
output Bronze / glob generik), jadi TIDAK ADA perubahan kode ingester
yang diperlukan, menjawab 2 dari 3 pertanyaan terbuka yang
KNOWN_RISKS.md RISK-15 sendiri tinggalkan. `config/schemas/
fred_macro.yaml` (SchemaValidator) juga generik, tidak per-domain.
KNOWN_RISKS.md RISK-15 status diupdate ke RESOLVED dengan detail
penuh; footer "Last updated" juga diperbaiki — ditemukan stale di
v1.13.4 (entry v1.13.5 tidak pernah menambahkan barisnya sendiri di
footer ini meski RISK-11-nya sendiri sudah diupdate) — entry v1.13.5
yang hilang ditambahkan sekalian.

### FIX IronOreSeriesID [config/fred_series.yaml, scripts/preflight/check_fred_commodity_series.py] — `PIORECRORECUSDM` Tidak Pernah Ada, Series Asli `PIORECRUSDM`

**Root cause**: web-verifikasi keenam series kandidat terhadap
fred.stlouisfed.org langsung (bukan asumsi) menemukan Iron Ore's ID
asli adalah `PIORECRUSDM` — BUKAN `PIORECRORECUSDM` yang dikutip
Architecture Extension v1.0 ADR-005 DAN KNOWN_RISKS.md RISK-15 sendiri
sejak 25 Juni 2026. `PIORECRORECUSDM` tidak eksis di FRED — typo
duplikasi "ORE" (`PIORE-CR-ORE-C-USDM` vs yang asli `PIORE-CR-USDM`)
yang terbawa tanpa terverifikasi lintas dua dokumen desain dan risk
entry ini sendiri — persis kelas gap yang RISK-15 sendiri bunyikan
alarmnya. 5 series lain (`PCOALAUUSDM`, `PPOILUSDM`, `PRUBBUSDM`,
`PTINUSDM`, `PNICKUSDM`) dikonfirmasi benar sesuai dokumentasi.

**Fix**: `PIORECRUSDM` (bukan `PIORECRORECUSDM`) yang ditulis ke
`fred_series.yaml` dan `check_fred_commodity_series.py`. Regression
guard permanen ditambahkan
(`test_iron_ore_series_id_is_not_the_documented_typo`) yang secara
eksplisit assert `PIORECRORECUSDM` TIDAK ADA di
`EXPECTED_COMMODITY_SERIES` — bukan cuma assert yang benar ada.

### ADD CheckFredCommoditySeries [scripts/preflight/check_fred_commodity_series.py, tests/unit/test_preflight_scripts.py, tests/COUNT_BASELINE.txt] — Preflight Script Baru untuk FRED

**Root cause**: KNOWN_RISKS.md RISK-15 sendiri menyarankan "author or
extend a preflight script ... mirroring check_yfinance_tickers.py's
pattern." Listing `scripts/preflight/` dicek langsung — tidak ada
script FRED sama sekali di antara 4 yang ada (`check_bis_cbpol_d.py`,
`check_bis_eer_weights.py`, `check_finnhub_shape.py`,
`check_yfinance_tickers.py`) — jadi diauthor baru, bukan extend.

**Fix**: `check_fred_commodity_series.py` ditulis mengikuti pola
persis `check_bis_cbpol_d.py` — `EXPECTED_COMMODITY_SERIES` dict
independen (bukan import dari `fred_series.yaml`), `--series` flag
untuk filter satu series, exit code 0/1. Constraint jaringan sama
seperti setiap preflight script lain di proyek ini:
`api.stlouisfed.org` tidak pernah ada di allowlist sandbox manapun —
authoring tidak butuh akses jaringan, running-nya yang butuh,
dijalankan Ovi nanti di M1.

**Diverifikasi empiris**: 10 test baru (`TestCheckFredCommoditySeries`)
dijalankan offline di sandbox terisolasi — seluruhnya pass. Digabung
dengan 4 class test preflight-script existing dan di-collect ulang:
**42 test total collected, 0 collection error** — kelas kegagalan
persis yang CI/CD Ops Guide sendiri (Gate G-4) bunyikan alarmnya
(NEW-4). Setiap file write di-closed-loop-verify: dibaca ulang dari
repo live setelah tiap edit dan di-diff terhadap versi tervalidasi
sandbox sebelum lanjut ke file berikutnya.

**Belum dijalankan**: invocation nyata `check_fred_commodity_series.py`
dengan `FRED_API_KEY` terhadap live FRED, dan full `poetry run pytest`
di hardware nyata — keduanya perlu terjadi di sana sebelum ini
dianggap exercised end-to-end. `tests/COUNT_BASELINE.txt`: 1422 →
1432.

---

## v1.13.5 — `scripts/archive/` Dihapus Total, Test Suite Diperbaiki (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — kelanjutan
dari v1.13.4, thread baru. Ovi melaporkan "G-6 — Coverage Gate failure
karena archived items dibersihkan dari repository."

Total: **1 root cause diklarifikasi** (bukan coverage, tapi test
failure yang menumpang command yang sama) | **10 test usang
ditertirkan, 1 test masih valid dipertahankan** | **1432 → 1422
passed / 0 failed / 0 error**.

### CLARIFY G6Label [tidak ada file diubah] — Bukan Coverage Gate, Tapi Test Pass Gate

**Root cause**: menjalankan command CI-equivalent persis
(`pytest tests/ --cov=src --cov-report=term-missing`) terhadap state
nyata (`b013a6a`, sudah sync, tidak perlu overlay). Hasil: **coverage
81.54%**, di atas gate 80% dengan jelas — pesan `Required test
coverage of 80.0% reached` muncul eksplisit. Kegagalan sebenarnya:
**7 failed, 1425 passed**, seluruhnya di satu file:
`tests/unit/test_archived_migration_scripts.py`. Wrapper yang
melaporkan ini sebagai "G-6" kemungkinan besar karena satu command
gabungan `pytest --cov` menjalankan test SEKALIGUS mengukur coverage
— test failure menjatuhkan exit code seluruh command meski angka
coverage-nya sendiri lolos. Presisi penting di sini: ini masalah G-5
(test pass) yang berlabel G-6, bukan regresi coverage sungguhan.

### FIX ArchiveTests [tests/unit/test_archived_migration_scripts.py → tests/unit/test_makefile_safety_nets.py, tests/COUNT_BASELINE.txt] — Regression Guard yang Targetnya Sudah Tidak Ada, Diretired

**Root cause**: `test_archived_migration_scripts.py` (RISK-11, 11
test) adalah regression guard permanen yang memverifikasi KEBERADAAN
archive — README ada, kedua script archived masih syntactically
valid, bare `import scripts.archive.X` masih memicu guard
`SystemExit("ARCHIVED ...")` yang spesifik. Ovi menghapus
`scripts/archive/` total (9 file, ~3.309 baris) di commit yang sama
dengan rilis v1.13.4 (`b013a6a`) sebagai cleanup lanjutan. 7 dari 11
test gagal persis sesuai desainnya ketika precondition-nya jadi salah
— `AssertionError` pada README yang hilang, `FileNotFoundError` pada
`ast.parse()` yang membaca file yang tidak ada, dan
`ModuleNotFoundError: No module named 'scripts.archive'` bukan
`SystemExit` yang diharapkan.

**Bukan regresi**: bug yang dijaga file test ini — destructive
import-time write dari `migrate_instruments.py` /
`build_instruments_v14.py` — sekarang mustahil secara struktural,
bukan sekadar dinonaktifkan: file-nya tidak ada di manapun lagi, baik
archived maupun tidak. Coverage sendiri tidak terpengaruh
(`scripts/archive/` tidak pernah masuk
`[tool.coverage.run] source = ["src"]`).

**Fix**: dari 11 test, tepat 1 yang masih menguji sesuatu yang benar
terlepas dari nasib archive — safety net `make migrate` (masih ada di
Makefile, masih harus gagal keras). `test_archived_migration_scripts.py`
di-overwrite in-place dengan hanya class tersebut
(`TestMakefileMigrateTargetFailsLoudly`), lalu di-rename menjadi
`test_makefile_safety_nets.py` agar nama file mencerminkan isinya yang
sebenarnya sekarang. 10 test yang premisnya sudah tidak ada diretired.

**Diverifikasi empiris**: divalidasi di sandbox terisolasi dulu (file
trimmed dijalankan standalone — 1 passed) sebelum diterapkan ke repo
live. Full suite setelah fix: **1422 passed, 0 failed**. Coverage:
**81.43%**, tidak berubah (tidak ada `src/` yang disentuh rilis ini).
Gates G-1 (156 file — turun dari 164, cocok persis dengan 8 file `.py`
yang hilang dari `scripts/archive/`), G-2 (0 f-string SQL), G-3 (699
symbols, tidak terpengaruh), G-8 (0 glob-scope violations) semua
bersih. `tests/COUNT_BASELINE.txt` diupdate ke 1422.

**Catatan proses**: satu `edit_file` call ke `KNOWN_RISKS.md` di
rilis ini sempat submit tanpa `dryRun` eksplisit dan diam-diam tidak
diterapkan (default tampaknya `true`) — ketahuan karena dibaca ulang
dan di-grep untuk heading baru sebelum diasumsikan berhasil, bukan
percaya diff yang dikembalikan begitu saja. Di-retry dengan
`dryRun: false` eksplisit, lalu diverifikasi ulang via read-back.

---

## v1.13.4 — Gate 1 Discovery Phase Live-Confirmed, Key Shape `.N.B.` Terverifikasi, poetry.lock Diperbaiki (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — kelanjutan
langsung dari v1.13.3 dalam thread yang sama. Ovi menyerahkan log
preflight M1 baru (4 Agustus 2026) sebagai bukti project-knowledge, dan
terpisah melaporkan "isu baru pyproject.toml" tanpa pesan error
terlampir.

Total: **2 item dari RISK-16 ditutup** (Gate 1 discovery phase
live-confirmed; key shape `.N.B.` live-re-confirmed) | **1 bug
build-tooling ditemukan dan diperbaiki** (poetry.lock content-hash
stale) | **1432 passed / 0 failed / 0 error** (baseline tidak berubah).

### CONFIRM Gate1 [scripts/preflight/check_bis_eer_weights.py, KNOWN_RISKS.md] — `--discover-weights` Dijalankan Nyata di M1, Layout File Sekarang Diketahui Penuh

Ovi menjalankan `check_bis_eer_weights.py --discover-weights` di M1 —
pertama kalinya sandbox manapun di proyek ini punya rute ke `bis.org`.
Hasil: `weightsb.xlsx` terunduh bersih (492.941 byte), 10 sheet
(`1993_1995` hingga `2020_2022`, mengkonfirmasi langsung siklus revisi
3-tahunan yang disebutkan FAQ BIS — belum ada vintage lebih baru dari
2020-22). Setiap sheet adalah matrix simetris "siapa memberi bobot ke
siapa" (baris = negara, kolom = mata uang yang diberi bobot, cell =
persentase bobot); scan menemukan seluruh 13 kode
`BROAD_DOLLAR_REF_AREAS` hadir sebagai baris MAUPUN kolom, di posisi
identik, di seluruh 10 sheet.

**Progress nyata, belum penutupan penuh**: scan ini membuktikan layout
persis seperti yang didesain `_discover_weights()` untuk dideteksi
tanpa asumsi, dan memberikan koordinat (row, col) yang diperlukan
untuk ekstraksi tertarget. TIDAK memberikan nilai weight aktual untuk
di-wire ke `BIS_WEIGHTS` — masih perlu pass lanjutan yang (a)
menemukan baris "US" secara spesifik (bukan salah satu dari 13 kode
target, sehingga tidak muncul di scan ini), dan (b) membaca nilai
baris tersebut di 13 kolom target. Gate 1 tetap terbuka, bergeser dari
"file ditemukan, layout tidak diketahui" menjadi "file ditemukan,
layout terkarakterisasi penuh, siap untuk ekstraksi."

### CONFIRM TypeKey [scripts/preflight/check_bis_eer_weights.py, KNOWN_RISKS.md] — Key Shape `.N.B.` Live-Terverifikasi

Ovi menjalankan ulang `check_bis_eer_weights.py` polos (tanpa flag).
Seluruh 13 currency PASS — tapi pada **3.813.875 byte per currency**,
~16x dari 237.188 byte yang tercatat untuk check 13-currency yang sama
di bawah key `M..B.` lama (KNOWN_RISKS.md, 3 Agustus). Lonjakan
tersebut persis yang seharusnya dihasilkan wildcard FREQ (dibanding
fix ke `M`): query sekarang mengambil frekuensi apapun yang
benar-benar dimiliki BIS per negara — untuk 13 currency ini, itu
daily — bukan dibatasi artifisial ke monthly. Ini menutup gap "belum
live-re-confirmed terhadap key shape ini" dari v1.13.3 secara penuh;
bukan sekadar re-test perilaku lama, delta byte-count itu sendiri
adalah bukti key shape baru melakukan sesuatu yang struktural berbeda,
ke arah yang diharapkan.

`--discover` (structure endpoint) dan `check_bis_cbpol_d.py` juga
dijalankan ulang: 568.951 byte dan 12/12 `daily-resolution=True` —
byte-count structure endpoint identik dengan angka 3 Agustus yang
sudah tercatat di KNOWN_RISKS.md, dan rentang obs-count CBPOL_D
(minimum 6.775 KR, maksimum 24.850 JP) serta rentang tanggal terkini
(hingga 2026-07-29) keduanya cocok persis dengan yang sudah tercatat.
Konsisten dengan latency T+1–T+3 yang dinyatakan BIS sendiri, bukan
artefak re-run — tidak ada observasi baru yang terpropagasi dalam
selang satu hari untuk kedua belas central bank. Dibaca sebagai
konfirmasi stabilitas, bukan informasi baru.

### FIX poetry.lock [pyproject.toml, poetry.lock] — Content-Hash Stale, Ditemukan dan Diperbaiki

**Root cause**: "isu baru pyproject.toml" yang dilaporkan Ovi tidak
disertai error, jadi investigasi dimulai dari file itu sendiri, bukan
stack trace. Komentar dependency `openpyxl` 3 Agustus (lihat v1.13.3)
sudah memflag, di teksnya sendiri, bahwa edit pyproject.toml saja
tidak akan meregenerasi `poetry.lock`, dan tidak ada session sampai
titik itu yang punya akses shell untuk verifikasi atau perbaikan.
Dikonfirmasi via timestamp modifikasi `poetry.lock` (31 Juli, satu
hari SETELAH penghapusan tvdatafeed 30 Juli — run `poetry lock` manual
sudah menghapus tvdatafeed dengan benar) yang mendahului edit
`openpyxl` 3 Agustus. Direproduksi error sebenarnya di sandbox
terisolasi (poetry 2.4.1, akses network PyPI) terhadap file live yang
persis sama:

```
Error: pyproject.toml changed significantly since poetry.lock was last
generated. Run `poetry lock` to fix the lock file.
```

**Fix**: `tvdatafeed` sudah benar tidak ada di lock (run 31 Juli sudah
menanganinya); desync murni content-hash yang belum diregenerasi
setelah edit `openpyxl` berikutnya. Diregenerasi dengan `poetry lock`;
di-diff old vs new lock di level package: **113/113 package identik
nama dan versi** — hanya baris metadata `content-hash` yang berubah.
Tidak ada drift pandas/numpy/dll, tidak ada yang ter-upgrade diam-diam.
Diterapkan ke repo live sebagai perubahan `edit_file` satu baris
(dry-run di-diff, diterapkan, dibaca ulang, di-diff ulang terhadap
copy yang sudah diverifikasi sandbox — byte-identical). Catatan
provenance yang sudah tidak akurat ("not something this session can
execute") di komentar penghapusan tvdatafeed juga dikoreksi di tempat
untuk mencatat fix, metode, dan tanggal yang sebenarnya.

**Diverifikasi empiris**: clone GitHub main (`fce8be9`) di-spot-check
terhadap state lokal yang sudah diketahui benar sebelum dipercaya:
versi 1.13.3, tvdatafeed sudah diarsipkan dengan benar, HKD/TWD/NOK
hadir di kedua file config instrumen, `openpyxl` dideklarasikan —
semua cocok. Hanya hash `poetry.lock` yang stale di sana juga, persis
seperti diharapkan (fix belum di-push). `pyproject.toml`/`poetry.lock`
yang sudah diverifikasi di-overlay ke clone tersebut dan dijalankan
sungguhan, bukan dry-run: `poetry install --with dev` → seluruh 113
package, `alpha-factory 1.13.4` terinstall editable, bersih. `poetry
run pytest tests/ -q` → **1432 passed, 0 failed, 0 error** — cocok
persis dengan `tests/COUNT_BASELINE.txt` (tidak berubah rilis ini,
tidak ada test ditambah/dihapus). Coverage: 81.43%, tidak berubah,
masih di atas gate 80%. Gates G-1 (164 file, 0 syntax error), G-2 (0
f-string SQL), G-3 (699 symbols, tidak terpengaruh), G-8 (0
glob-scope violations) semua di-re-run bersih terhadap install nyata.

---

## v1.13.3 — Gate 1 Weights File Located, TYPE Decision (Nominal), 13-Currency Live Re-confirmation (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — kelanjutan
langsung dari v1.13.2 dalam thread yang sama. Ovi meminta penyelesaian 3
item yang masih terbuka: dampak temuan Monthly-vs-Daily terhadap
ADR-010, Gate 1 (ADR-017/018 exact Broad Dollar weight components), dan
keputusan TYPE (Real vs Nominal) untuk EER query.

Total: **1 gap ditutup penuh** (13-currency EER live re-test, plus
konfirmasi Monthly-vs-Daily tidak berdampak ke ADR-010 untuk CB manapun)
| **1 progress substansial** (Gate 1 — file weight asli BIS ditemukan
dan dikonfirmasi reachable, belum di-parse) | **1 keputusan arsitektur
diambil** (TYPE=Nominal) | **1427 → 1432 passed / 0 failed / 0 error**.

### CONFIRM BIS-1-cont [scripts/preflight] — 13-Currency EER Live Re-test + Penutupan Pertanyaan Monthly-vs-Daily/ADR-010

Ovi menjalankan ulang preflight scripts terhadap kode v1.13.2. Hasil:
**`check_bis_eer_weights.py` — seluruh 13 REF_AREA code PASS**, 237.188
byte per check (naik dari 182.410 byte saat masih 10 currency —
response lebih besar karena key lebih lebar). **`check_bis_cbpol_d.py`
— hasil identik dengan run sebelumnya**, 12/12 `daily-resolution=True`,
mengkonfirmasi stabilitas lintas run. Ini menutup gap "belum di-re-test
terhadap 13 currency" dari v1.13.2 secara penuh, dan sekaligus **menjawab
definitif pertanyaan yang dulu dibiarkan terbuka**: temuan Monthly-vs-
Daily TIDAK berdampak ke rasional ADR-010 untuk CB manapun dari 12 CB —
seluruhnya terkonfirmasi daily-resolution secara empiris pada dua run
terpisah (1 Aug dan 3 Aug).

### ADD Gate1 [scripts/preflight/check_bis_eer_weights.py, pyproject.toml] — File Weight BIS Asli Ditemukan (Belum Ditutup Penuh)

**Root cause / temuan**: GMI v6 mengasumsikan weight components mungkin
tidak bisa diakses via API apapun ("dokumentasi artifact, bukan queryable
SDMX series"). Web research thread ini menemukan jawaban sebenarnya:
halaman `data.bis.org/topics/EER` (server-rendered, berbeda dari
halaman JS-SPA lain di proyek ini) link langsung, di section
"Methodology"-nya sendiri, ke tabel weight yang bisa di-download:
`https://www.bis.org/statistics/eer/weightsb.xlsx` (Broad, 64 economies
— dikonfirmasi yang benar, karena Narrow hanya mencakup 26/27 economy
inti dan akan mengecualikan IDR/HKD/TWD). Dikonfirmasi reachable dan
genuinely .xlsx (mime type
`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`,
bukan redirect/error page) via `web_fetch`. Juga dikonfirmasi: weight
bersifat **time-varying per 3 tahun** (vintage 1993-95 hingga 2017-19
per FAQ resmi BIS; vintage 2017-19 dipakai terus-menerus untuk "periode
terbaru" hingga update 3-tahunan berikutnya dipublikasikan) — tidak ada
"exact weight" permanen tunggal, tapi ADA vintage spesifik yang bisa
disebutkan.

**Fix**: `check_bis_eer_weights.py` mendapat mode baru `--discover-weights`
— download file, laporkan sheet name/dimension asli, sample struktur,
dan scan untuk currency/REF_AREA code kita sendiri — sengaja TIDAK
mengasumsikan layout row/column (menebak layout berisiko sama dengan
masalah yang sudah 3x terjadi di thread BIS ini: WS_CBPOL_D, WS_EER_M,
segment `structure/` yang hilang — tiga tebakan percaya diri yang
ternyata salah). Pola two-phase discover-then-extract yang sama seperti
`--discover` untuk struktur API. `openpyxl` dipromosikan dari transitive
ke direct dependency eksplisit (pola yang sama seperti `jsonschema` di
Decision B Step 3).

**Belum ditutup**: TIDAK bisa parse isi xlsx sungguhan melalui tool yang
tersedia — `web_fetch` mengembalikannya sebagai binary buram, dan
`bis.org` tidak ada di network allowlist sandbox manapun di proyek ini.
Ini progress nyata (file ditemukan, dikonfirmasi asli dan reachable)
tapi BUKAN penutupan penuh — nilai weight exact per currency masih
memerlukan seseorang dengan akses network untuk benar-benar membuka
file-nya. `_discover_weights()` diuji hanya terhadap workbook sintetis
di dalam test suite (membuktikan logic scanning-nya benar, bukan bahwa
ia akan menemukan sesuatu yang berguna di layout asli BIS yang belum
diketahui).

### DECIDE TYPE [scripts/preflight/check_bis_eer_weights.py] — Nominal, Bukan Real

**Keputusan**: TYPE dikunci ke Nominal, sebelumnya sengaja dibiarkan
wildcard menunggu keputusan. Dua alasan independen yang saling
memperkuat: (1) DXY sendiri — index yang secara eksplisit menjadi basis
desain Broad Dollar Index platform ini (Architecture v2.0 §7.2) —
adalah index nominal, bukan inflation-adjusted; memakai Real EER untuk
Broad Dollar sementara DXY tetap Nominal akan membandingkan dua konsep
berbeda di bawah satu payung "kekuatan Dollar". (2) Halaman
`data.bis.org/topics/EER` yang sama menyatakan data EER frekuensi Daily
HANYA tersedia untuk index Nominal, tidak pernah Real — dan Layer 2
anchor platform ini didesain cadence Daily (Architecture v2.0 §7.2), jadi
Nominal adalah satu-satunya pilihan yang bisa benar-benar memberikan itu.

**Fix**: FREQ diubah dari fixed `M` menjadi wildcard (mencerminkan logic
yang sama persis dengan `check_bis_cbpol_d.py`: minta frekuensi apapun
yang BIS benar-benar punya per negara, bukan asumsi, sehingga data daily
yang genuinely tersedia bisa masuk tanpa risiko false failure pada
currency yang mungkin hanya punya EER bulanan). Constant di-rename
`BIS_EER_ENDPOINT_MONTHLY` → `BIS_EER_ENDPOINT` (sudah tidak akurat
disebut "monthly-only"). Key berubah dari `M..B.` menjadi `.N.B.`.

**Belum diverifikasi live**: konfirmasi live 13-currency di atas memakai
struktur key `M..B.` yang lama, sebelum keputusan TYPE ini
diimplementasikan. Re-test terhadap key `.N.B.` yang baru adalah langkah
berikutnya yang jelas.

**Diverifikasi empiris**: diuji penuh di sandbox clone (Python 3.12)
sebelum diterapkan ke repo nyata. Full suite: 1427 → 1432 passed, 0
failed (5 test baru: 1 untuk URL file weight, 4 untuk `_discover_weights()`
— download failure, unparseable content, synthetic-workbook scan,
CLI wiring). Coverage: 81.43% (tidak berubah, > gate 80%). Gates
G-1/G-2/G-3/G-8 semua re-run bersih. Diterapkan ke repo nyata via
whole-file overwrite (bukan targeted edit) mengingat skala diff
(~330 baris di script utama) — setelah insiden ambiguous-match di
v1.13.2, ini dinilai jalur risiko lebih rendah untuk diff seukuran ini,
bukan jalan pintas dari verifikasi.

**Catatan proses**: dev-log thread ini sebelumnya salah menggabungkan
konten v1.13.1/v1.13.2/v1.13.3 ke satu file yang terus di-edit,
melanggar konvensi proyek sendiri ("satu file dev-log per rilis, tidak
pernah dimodifikasi setelah dibuat"). Diperbaiki: dipecah menjadi 3 file
terpisah, file gabungan yang salah dipindah ke `dev-log/archive/`
(bukan dihapus). Dicatat di sini untuk transparansi, bukan disembunyikan.

---

## v1.13.2 — FIX BIS-1 Confirmed Live + HKD/TWD/NOK Dollar Basket Completion (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — kelanjutan
langsung dari v1.13.1 dalam thread yang sama. Ovi menjalankan 4 preflight
module di M1 hardware, menghasilkan bukti live pertama untuk fix BIS-1,
lalu meminta penyelesaian gap HKD/TWD/NOK yang sudah diflag sejak thread
28 Jul 2026.

Total: **1 konfirmasi live** (BIS CBPOL/EER endpoint terbukti benar
terhadap live API, bukan hanya test suite) | **1 gap ditutup** (HKD/TWD/
NOK, 10→13 currency di Broad Dollar basket) | **1 structural fix
tambahan** (endpoint key sekarang derive dari dict, bukan literal
terpisah) | **1426 → 1427 passed / 0 failed / 0 error**.

### CONFIRM BIS-1 [KNOWN_RISKS.md RISK-16] — BIS CBPOL/EER Endpoint Terkonfirmasi Live, Bukan Hanya Lolos Test Suite

Ovi menjalankan seluruh 4 preflight module di M1 (`check_yfinance_tickers.py`,
`check_finnhub_shape.py`, `check_bis_eer_weights.py`, `check_bis_cbpol_d.py`)
terhadap kode yang sudah diperbaiki v1.13.1. Hasil: **`check_bis_cbpol_d.py`
— seluruh 12 REF_AREA code PASS dengan `daily-resolution=True`**, observation
count nyata (6.775–24.850 per negara) dan tanggal terkini (hingga
2026-07-29). **`check_bis_eer_weights.py --discover` — berhasil**, fetch
568.951 byte struktur dataflow nyata dari endpoint `structure/dataflow/
BIS/WS_EER/1.0` (sebelumnya 501). **`check_bis_eer_weights.py` — seluruh
10 REF_AREA code (versi sebelum ekspansi HKD/TWD/NOK) PASS**, 182.410
byte per check.

Ini menutup temuan sampingan v1.13.1 yang mengkhawatirkan: sampling awal
via `data.bis.org` portal (4 dari 12 CB kita — GB/CH/NO/JP) tampak
Monthly-only, berlawanan dengan rasional ADR-010. Query API nyata
(dengan FREQ wildcard sesuai desain fix) ternyata mengembalikan data
Daily untuk seluruh 12 CB termasuk ECB/XM — rasional ADR-010 kini
terkonfirmasi benar secara empiris, bukan hanya diasumsikan atau
sebagian bertentangan seperti dikhawatirkan sebelumnya.

**KNOWN_RISKS.md RISK-16 status**: FIXED (code), pending live confirmation
→ **RESOLVED (confirmed live)**.

### FIX BIS-1-cont [scripts/preflight/check_bis_eer_weights.py] — HKD/TWD/NOK Melengkapi Broad Dollar Basket (10→13 Currency)

**Root cause**: thread 28 Jul 2026 secara eksplisit meng-flag bahwa
desain Broad Dollar basket saat ini (per komentar `instruments_taxonomy.yaml`
`dollar` + `dollar_basket` groups) sebenarnya 13 currency (6 asli + IDR +
6 currency `context_dollar_basket`: CNH/KRW/SGD/HKD/TWD/NOK), bukan 10
yang di-cover script saat itu — tapi sengaja TIDAK ditambahkan karena
"Ovi's instruction was specifically MXN->IDR". Ovi kini meminta gap ini
ditutup secara eksplisit.

**Fix**: `BROAD_DOLLAR_REF_AREAS` ditambah HKD→HK, TWD→TW, NOK→NO (13
total). **Ditemukan sekaligus diperbaiki dalam proses**: endpoint key
(`BIS_EER_ENDPOINT_MONTHLY`) sebelumnya berupa literal string terpisah
dari dict — menambah entry ke dict saja TANPA fix ini akan membuat 3
currency baru permanen tidak pernah ter-fetch, sementara `_check_one()`
tetap percaya diri melaporkan "not present" — tidak bisa dibedakan dari
kegagalan API sungguhan. Endpoint key sekarang dibangun langsung dari
`BROAD_DOLLAR_REF_AREAS.values()` (`"+".join(...)`), membuat seluruh
kelas bug drift-antara-dict-dan-key ini mustahil terjadi lagi secara
struktural.

**Diverifikasi empiris**: diuji di sandbox clone yang sama (Python 3.12)
sebelum diterapkan ke repo nyata via filesystem connector. Full suite:
1426 → 1427 passed, 0 failed (1 test baru). Coverage: 81.43% (tidak
berubah, > gate 80%). **Belum di-re-run live terhadap versi 13-currency**
— konfirmasi live di atas mencakup versi 10-currency yang sudah
ditest; ekspansi +3 sudah diperbaiki di kode dan diverifikasi test
(termasuk test dinamis yang otomatis mencakup currency baru tanpa
perubahan test, plus 1 guard eksplisit baru) tapi belum dikonfirmasi
ulang secara live.

**Test baru**: `test_preflight_scripts.py::TestCheckBisEerWeights::
test_hkd_twd_nok_completes_dollar_basket` (kunci sengaja dibuat
eksplisit per-currency, bukan hanya mengandalkan test dinamis
`test_key_wildcards_freq_and_fixes_broad_basket` yang sudah otomatis
mencakup entry baru apapun di dict).

**Catatan proses**: penempatan awal test baru ini sempat salah masuk ke
class `TestCheckBisCbpolD` (bukan `TestCheckBisEerWeights`) karena nama
method `test_endpoint_uses_correct_dataflow_id` muncul di kedua class
dan string match pendek tidak unik — terdeteksi dan diperbaiki sebelum
verifikasi akhir, dicatat di sini untuk transparansi proses, bukan
disembunyikan.

---

## v1.13.1 — FIX BIS-1: BIS CBPOL/EER Endpoint Root-Cause Correction (Agustus 2026)

Dokumen referensi: tidak ada decision document terpisah — root cause
ditemukan dan diperbaiki dalam satu thread langsung, dipicu oleh
permintaan eksplisit Ovi ("resolving BIS issues") setelah dua thread
sebelumnya (GMI v6, thread 28 Jul 2026) gagal menutup 404/501 yang
sama. Menggunakan `alpha-factory_preflight_logs___29_July_2026.txt`
sebagai bukti bahwa fix "v1→v2" sebelumnya TIDAK cukup.

Total: **1 root cause ditemukan** (dataflow ID salah, bukan hanya
struktur URL) | **4 file source diperbaiki** (config/bis_cb_rates.yaml,
src/bronze/bis_rates_ingester.py, 2 preflight script) | **2 file test
diperbarui** (1 test lama direvisi, 6 test baru ditambahkan) | **1420
→ 1426 passed / 0 failed / 0 error**.

### FIX BIS-1 [config/bis_cb_rates.yaml, src/bronze/bis_rates_ingester.py, scripts/preflight/check_bis_cbpol_d.py, scripts/preflight/check_bis_eer_weights.py] — Dataflow ID Salah, Bukan Hanya Struktur URL

**Root cause**: thread 28 Jul 2026 memperbaiki struktur path v1→v2 tapi
mengklaim dataflow ID `WS_CBPOL_D`/`WS_EER_M` sudah "independently
confirmed correct" via sumber yang tidak spesifik ("a BIS SDMX Python
client's dataflow listing") — klaim ini tidak pernah benar-benar
diverifikasi, dan preflight log 29 Jul membuktikannya salah (404/501
tetap terjadi meski fix v1→v2 sudah diterapkan). Root cause sebenarnya,
ditemukan thread ini via web research (bukan live API call — tidak ada
sandbox proyek ini yang punya akses ke stats.bis.org): dataflow ID yang
benar adalah `WS_CBPOL` dan `WS_EER` — bukan `WS_CBPOL_D`/`WS_EER_M`.
Suffix "_D"/"_M" adalah label cadence (daily/monthly) yang salah
dikira bagian dari dataflow identifier; frekuensi sebenarnya adalah
KEY dimension (`FREQ.REF_AREA` untuk CBPOL, `FREQ.TYPE.BASKET.REF_AREA`
untuk EER), bukan bagian nama flow. Error kedua, independen: endpoint
`--discover` EER hilang segment path `structure/` sepenuhnya
(`/api/v2/dataflow/...` alih-alih `/api/v2/structure/dataflow/...`) —
konsisten dengan 501 (bukan 404 bersih) yang benar-benar dikembalikan.

**Evidence trail**: `data.bis.org` (portal resmi BIS) menampilkan 8
halaman negara untuk CBPOL (`topics/CBPOL/BIS,WS_CBPOL,1.0/{FREQ}.
{REF_AREA}`) dan 7 halaman untuk EER (`topics/EER/BIS,WS_EER,1.0/
{FREQ}.{TYPE}.{BASKET}.{REF_AREA}`), semuanya tanpa suffix "_D"/"_M".
Dikonfirmasi independen via contoh kode working nyata (blog
jamelsaadaoui.com/EconMacro, komentar live, untuk dataflow sibling
WS_CBTA) dan paper konferensi SDMX 2025 resmi (sdmx2025.org, untuk
dataflow sibling WS_XRU) — keduanya memakai bentuk URL yang identik
dengan yang sekarang dipakai di sini.

**Temuan sampingan, bukan diselesaikan thread ini**: dari 4 central
bank di 12-CB list kita yang ter-sample (GB/BOE, CH/SNB, NO/NORGES,
JP/BOJ), semuanya kembali sebagai Monthly, bukan Daily — berlawanan
dengan rasional asli ADR-010 ("BIS provides daily where FRED only has
monthly"). ECB/XM sendiri (bank yang jadi fokus ADR-010) tidak
ter-sample langsung. Diflag untuk review setelah endpoint yang
diperbaiki dikonfirmasi live, tidak diselesaikan di sini — lihat
KNOWN_RISKS.md RISK-16.

**Fix**: endpoint dikoreksi di 3 titik independen (config YAML +
production ingester yang hardcode endpoint-nya sendiri, tidak baca
config + 2 preflight script) menjadi
`https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/.XM+GB+
JP+CA+AU+NZ+CH+KR+NO+SE+CN+ID` (CBPOL) dan
`https://stats.bis.org/api/v2/data/dataflow/BIS/WS_EER/1.0/M..B.XM+
JP+GB+CA+CH+AU+ID+CN+KR+SG` (EER, key = FREQ.TYPE.BASKET.REF_AREA,
FREQ=M fixed karena tidak ditemukan varian daily, TYPE wildcarded
karena Real vs Nominal belum diputuskan, BASKET=B fixed sesuai "Broad
Dollar Index"). Endpoint `--discover` EER dikoreksi ke
`/api/v2/structure/dataflow/BIS/WS_EER/1.0`. Key CBPOL wildcard FREQ
(leading empty segment) alih-alih hardcode "D" — literal "all" sebagai
key segment (nilai lama) bukan sintaks SDMX key yang valid, dan
sampel negara menunjukkan campuran Monthly/Daily. Semantik pass/fail
`_daily_resolution()` di `check_bis_cbpol_d.py` DIBIARKAN TIDAK
BERUBAH secara sengaja — script sekarang akan melaporkan temuan per-
negara yang jujur terhadap data nyata, bukan sesuatu yang perlu
dilonggarkan sepihak.

**Diverifikasi empiris**: fix diterapkan dan diuji dua kali — sekali di
sandbox clone terisolasi (`git clone` dari GitHub, dikonfirmasi in-sync
dengan filesystem lokal via perbandingan `pyproject.toml`; Python 3.12,
`poetry install --with dev` bersih, 113 paket), sekali diterapkan
langsung ke repo nyata via filesystem connector setelah verifikasi
sandbox lulus. Full suite (sandbox): 1420 passed (baseline, sebelum
fix) → 1426 passed, 0 failed (setelah fix + test baru). Coverage:
81.41% → 81.43% (> gate 80%). Gate G-1 (164 file, 0 error), G-2 (0
f-string SQL), G-3 (699 symbols, Layer 1=640, Layer 2=59), G-8 (0
glob-scope violation) — semua PASS. **TIDAK dikonfirmasi terhadap
live BIS API** — tidak ada sandbox proyek ini yang punya network
access ke `stats.bis.org`; perlu dijalankan nyata via
`check_bis_cbpol_d.py`/`check_bis_eer_weights.py` di hardware M1
sebelum dianggap benar-benar closed. Lihat KNOWN_RISKS.md RISK-16
untuk detail lengkap.

**Test baru**: `test_bis_rates_ingester.py::TestBisEndpoint` (2 test),
`test_preflight_scripts.py::TestCheckBisCbpolD::test_endpoint_uses_
correct_dataflow_id` + `test_endpoint_key_wildcards_freq_and_includes_
all_ref_areas` (2 test), `TestCheckBisEerWeights::test_endpoint_uses_
correct_dataflow_id` + `test_structure_endpoint_uses_structure_prefix`
+ `test_key_wildcards_freq_and_fixes_broad_basket` (3 test, 1
menggantikan `test_endpoint_uses_v2_path_structure` yang lama —
direname, bukan dihapus, dengan docstring yang menjelaskan alasan).

---

## v1.13.0 — ADR-029–033: tvdatafeed Retirement & Layer 2 Proxy Adoption; Version/CHANGELOG Catch-Up (Juli 2026)

Dokumen referensi: `GMI_Decision_Document_v7.docx` (Decision I / ADR-029
tvdatafeed retirement; Decisions J–M / ADR-030–033 CPO/RUBBER/TIN/NICKEL
proxy adoption), menggunakan `alpha-factory_preflight_logs___29_July_2026.txt`
(OD-C1 kegagalan — sign-in gagal, non-IDX exchange fetch timeout meski
session "healthy") dan `alpha-factory verify-preflight logs — 30 July
2026.txt` (4 kandidat proxy PASS via `check_yfinance_tickers.py
--candidates`) sebagai empirical ground truth.

**Catatan versi/staleness:** `pyproject.toml` dan entri teratas
`CHANGELOG.md` ini sempat macet di `1.12.1` melewati beberapa thread nyata
yang landed di live main tanpa bump (ADR-027 instruments.yaml split, GMI
v6 Decision E preflight extension + G-6 trigger completion, ADR-028
poetry bootstrap check, thread 28 Jul 2026 preflight-fixes/coverage-
option-b, penambahan `check_yfinance_tickers.py --candidates`) —
dicatat di `GMI_Decision_Document_v7.docx` §4 sebagai "Ovi's call on
scope." Thread ini melakukan MINOR bump langsung (1.12.1 → 1.13.0),
bukan menebak angka PATCH 1.12.2/1.12.3/1.12.4 yang tidak pernah
benar-benar tercatat di manapun selain nama file zip informal — satu
lompatan bersih menghindari klaim presisi palsu tentang thread mana yang
"memiliki" digit PATCH yang mana.

Total: **tvdatafeed dipensiunkan sepenuhnya** (dependency, 2 modul
Bronze, 1 script preflight, 2 file test — semua diarsipkan/dihapus, RISK-1
→ RESOLVED) | **4 instrumen Layer 2 diaktifkan** (CPO, RUBBER, TIN, NICKEL
— dari 55 aktif/4 deferred menjadi 59 aktif/0 deferred, `count_total()`
695 → 699) | **1 risiko baru diflag** (RISK-15, `fred_series.yaml` gap
pre-existing, tidak diperbaiki thread ini). **Full test suite DIJALANKAN
sesi ini oleh Ovi** (`poetry-logs_v1_13_0.txt`, project knowledge) —
`poetry lock` sukses, `poetry run pytest`: **1 failed, 1418 passed**
pada pass pertama (1 regresi nyata ditemukan dan diperbaiki — lihat
"Correction" di bawah), coverage **81.41%** (> gate 80%),
`validate_instruments.py` PASSED (699 symbols, Layer 1=640, Layer 2=59),
Gate G-8 PASSED. Final count setelah correction: **1420** (lihat
"Catatan test count").

### FIX ADR-029 [src/bronze/market_ingester.py, source_adapter.py, yfinance_adapter.py, utils/health_reporter.py] — tvdatafeed Retirement

**Root cause / trigger**: `check_tvdatafeed_symbols.py` (29 Jul 2026, full
run) menunjukkan sign-in gagal ("error while signin"), client fallback ke
"nologin method", dan meski health check (BBCA 1D bar) melaporkan
"healthy", setiap fetch non-IDX exchange (BMDI, SGX, LME, ICE) timeout.
Pola ini dibaca sebagai structural nologin-mode access-tier gap, bukan
transient blip.

**Keputusan**: pensiunkan tvdatafeed sepenuhnya (bukan dipertahankan
sebagai fallback prioritas rendah) — `YFinanceJKAdapter` sudah menjadi
fallback ChainedAdapter teruji untuk IDX30 dan sekarang menjadi
satu-satunya source. CPO/RUBBER/TIN tidak pernah punya live tvdatafeed
wiring (`context_available: false` sepanjang waktu), jadi tidak ada
production dependency nyata yang terputus untuk mereka — hanya
config-intent (`tvfeed_symbol`/`tvfeed_exchange`, `ROUTING_TABLE`) yang
hilang, digantikan proxy yfinance (lihat FIX ADR-030–033 di bawah).

**Perubahan kode**:
- `market_ingester.py`: `idx_chain` dari `ChainedAdapter([TvDatafeedAdapter(), YFinanceJKAdapter()])`
  menjadi `ChainedAdapter([YFinanceJKAdapter()])`; `_primary_source_for()`
  case idx: `"tvdatafeed"` → `"yfinance"`; import `TvDatafeedAdapter`
  dihapus.
- `source_adapter.py`, `yfinance_adapter.py`: docstring/usage example
  diperbarui, tidak lagi mereferensikan pola 2-adapter tvdatafeed→yfinance.
- `health_reporter.py::_check_idx_coverage()`: direwrite dari
  tvdatafeed-vs-fallback menjadi presence-vs-missing. Field lama
  `idx_tvdatafeed_count`/`idx_fallback_count` dihapus, field baru
  `idx_present_count` ditambahkan. Query DuckDB disederhanakan (tidak lagi
  butuh `ROW_NUMBER()` per-source resolution — cukup `SELECT DISTINCT
  _symbol`). Ini BUKAN perubahan kosmetik: di bawah schema lama, setiap
  symbol yang present akan selalu tampil sebagai "fallback" (karena
  sumber satu-satunya sekarang adalah yfinance_jk), sehingga
  `IDX_COVERAGE_ALERT_THRESHOLD` akan ter-trip di SETIAP run sehat —
  false alarm permanen jika tidak diperbaiki.

**Arsip** (tidak ada import-time side effect, plain move — beda dengan
precedent RISK-11 yang butuh `SystemExit` guard):
- `src/bronze/tvdatafeed_adapter.py`, `tvdatafeed_session.py` →
  `scripts/archive/`.
- `scripts/preflight/check_tvdatafeed_symbols.py` → `scripts/archive/`.
- `tests/unit/test_tvdatafeed_adapter.py` (28 test, termasuk 1
  parametrize×4), `test_tvdatafeed_session.py` (35 test) → dipindah ke
  `scripts/archive/ARCHIVED_test_*.py` (prefix `test_` dihapus dari nama
  file agar tidak pernah ter-collect pytest — meski `testpaths=["tests"]`
  di `pyproject.toml` sudah membuat ini secara teknis tidak perlu, tetap
  dilakukan sebagai defense-in-depth murah).
- `tests/unit/test_preflight_scripts.py::TestCheckTvdatafeedSymbols` (5
  test) dihapus sepenuhnya (bukan dipindah — kelas test, bukan file
  utuh).
- `scripts/archive/README.md` diperbarui: section baru "tvdatafeed
  retirement (ADR-029)" mendokumentasikan kelima file di atas, terpisah
  dari framing "destructive migration scripts" yang sudah ada (kategori
  risiko yang berbeda — tvdatafeed modules tidak pernah punya destructive
  write path).

**pyproject.toml**: dependency git `tvdatafeed` dihapus.
**`poetry.lock` — DIJALANKAN, DIKONFIRMASI (`poetry-logs_v1_13_0.txt`)**:
`poetry lock` dijalankan Ovi setelah edit ini diterapkan (24.5s, resolve
bersih) — entry `tvdatafeed` dan transitive dependency-nya benar-benar
terhapus dari lockfile, bukan cuma dari teks `pyproject.toml`. (Catatan
audit: paragraf ini awalnya berbunyi "PENTING — belum bisa dieksekusi
sesi ini" saat entri ini pertama ditulis, sebelum `poetry-logs_v1_13_0.txt`
tersedia — diperbarui di sini, bukan di entri terpisah, karena ini masih
delivery yang sama, belum pernah di-tag/push.)

**KNOWN_RISKS.md**: RISK-1 header/status diubah menjadi RESOLVED, section
baru "Resolution (ADR-029, 30 Jul 2026)" ditambahkan sebelum
"Long-term migration path" (yang sekarang sudah dieksekusi, bukan lagi
roadmap item).

### FIX ADR-030–033 [config/instruments_identity.yaml, instruments_taxonomy.yaml] — CPO/RUBBER/TIN/NICKEL Proxy Adoption

**Root cause / trigger**: dengan tvdatafeed pensiun (ADR-029), keempat
instrumen ini butuh source baru atau tetap deferred selamanya. Riset
kandidat proxy ekuitas, dikonfirmasi live via
`check_yfinance_tickers.py --candidates` (30 Jul 2026): F34.SI (Wilmar
International, SGX — proxy CPO), STA.BK (Sri Trang Agro-Industry, SET —
proxy RUBBER), AFM.V (Alphamin Resources, TSX Venture — proxy TIN),
NIC.AX (Nickel Industries Ltd, ASX — proxy NICKEL).

**Perubahan per instrumen** (`context_available`/`include_in_forecast`:
`false` → `true`; `deferred_reason`/`planned_wave` dihapus):

| Symbol | yfinance_symbol (baru) | proxy_instrument | Catatan |
| --- | --- | --- | --- |
| CPO | F34.SI | F34.SI | `requires_fx_normalization` `true`→`false` (bukan lagi raw MYR commodity feed) |
| RUBBER | STA.BK | STA.BK | Preflight hanya 3/5 rows — kemungkinan libur bursa Thailand, belum dikonfirmasi independen |
| TIN | AFM.V | AFM.V | Monitored risk: headline "CIRO trade resumption" (~Jan 2026) belum diinvestigasi |
| NICKEL | NIC.AX | NIC.AX | `structural_break` (LME suspension 2022-03-07) dipertahankan — masih relevan sebagai konteks pasar nickel |

**Keputusan desain yang sengaja TIDAK dilakukan**:
- `proxy_for`/`proxy_correlation_expected` **sengaja tidak diset** untuk
  keempat instrumen — `validate_instruments.py` mewajibkan
  `proxy_correlation_expected` setiap kali `proxy_for` ada, dan belum ada
  analisis korelasi empiris proxy-vs-komoditas untuk satupun dari empat
  ini (berbeda dari VALE yang punya angka ~0.81 dari studi nyata).
  Follow-up, bukan fabrikasi angka.
- `base_currency` dihapus untuk keempat instrumen (sebelumnya
  MYR/USD/USD/tidak-ada) — mengikuti presedan VALE/WHC.AX yang tidak
  membawa flag currency meski trading dalam mata uang asing (SGD, THB,
  CAD, AUD masing-masing) — pola equity-proxy, bukan raw
  currency-denominated commodity feed.
- `config/fred_series.yaml` **sengaja tidak disentuh** — lihat
  `KNOWN_RISKS.md` RISK-15 (baru).

**Dampak berantai — Layer 2 universe**:
`InstrumentLoader.count_context()`: 55 → 59. `deferred_count()`: 4 → 0.
`count_total()`: 695 → 699 (640 Layer 1 + 59 Layer 2, kembali ke ceiling
699 sebelum deferral NICKEL). `by_context_group("commodity")` tetap 11
(invariant terhadap split aktif/deferred).

**Test diperbarui** (semua rename 1:1 atau perubahan assertion pada test
existing — nol test baru ditambahkan, lihat "Catatan test count"):
`test_context_anchors.py` (2 test: 55→59, `test_resolve_excludes_deferred`
→ `test_resolve_no_instruments_currently_deferred` dengan assertion
dibalik), `test_instrument_loader.py` (9 test: count 55→59/695→699/4→0,
`test_deferred_instruments_have_required_fields` →
`test_no_deferred_instruments_remain`, `test_forecast_context_excludes_deferred`
→ `test_forecast_context_now_includes_former_deferred`,
`test_adr023_only_cpo_is_myr_dependent` →
`test_adr023_history_superseded_by_adr030_033`), `test_full_system.py`
(1 test: `test_l7_layer2_context_universe_present`, 55/4 → 59/0).

### Housekeeping [pyproject.toml] — Version Bump + Stale Coverage Threshold

- `version = "1.12.1"` → `"1.13.0"` (MINOR — lihat catatan staleness di
  atas untuk rationale).
- `[tool.coverage.report] fail_under = 70` → `80`. Ditemukan saat sudah
  membuka file ini untuk alasan lain: `ci.yml` sudah menegakkan
  `--cov-fail-under=80` sejak thread 28 Jul 2026 (Decision F Option B,
  `GMI_Decision_Document_v6.docx` §4), tapi field ini di `pyproject.toml`
  tidak pernah diupdate untuk match — artinya `pytest --cov=src
  --cov-fail-under` tanpa override CLI eksplisit akan diam-diam
  menegakkan threshold yang salah (lebih longgar). Tidak ada perubahan
  perilaku CI aktual (flag eksplisit `ci.yml` sudah override field ini),
  murni menghapus angka yang terlihat menegakkan sesuatu padahal tidak
  mencerminkan realita — kelas gap yang sama dengan riwayat trigger G-6
  (`GMI_Decision_Document_v6.docx` §1.1).

### ADD RISK-15 [KNOWN_RISKS.md] — FRED Track 2 Supplement Gap (Ditemukan, Tidak Diperbaiki)

Ditemukan insidental saat membaca `config/fred_series.yaml` untuk thread
ini: `PIORECRORECUSDM`/`PCOALAUUSDM` (ADR-005/006's FRED monthly
supplement untuk IRON_ORE/COAL_NEWC) tidak pernah benar-benar ditambahkan
ke file live, meski sudah diputuskan sejak Architecture Extension v1.0.
4 series kandidat baru (`PPOILUSDM`/`PRUBBUSDM`/`PTINUSDM`/`PNICKUSDM`)
untuk CPO/RUBBER/TIN/NICKEL TIDAK ditambahkan dalam thread ini — di luar
scope tvdatafeed retirement, dan `fred_ingester.py`'s domain-parsing
logic belum diverifikasi mendukung domain `commodity`. Detail lengkap
dan suggested next step di `KNOWN_RISKS.md` RISK-15.

### Catatan test count

**1487 → 1419 → 1420 (final)**. 1487→1419 (Δ -68) murni dari
pengarsipan/penghapusan test file untuk kode yang dipensiunkan (28 + 35 +
5, lihat FIX ADR-029 di atas) — bukan regresi. 1419→1420 (Δ +1) dari
correction di bawah: 1 rename 1:1 + 1 test baru genuinely ditambahkan
(bukan nol seperti klaim awal thread ini — lihat "Correction"). Final:
**1420**, cocok dengan real run kedua (1419 collected pada percobaan
pertama, +1 dari test baru correction, belum di-re-run empiris tapi
arithmetic straightforward). `tests/COUNT_BASELINE.txt`: 1487 → 1419 →
**1420**.

### Correction (sesi yang sama, sebelum dianggap selesai) — `test_is_deferred_property` Terlewat

**Ditemukan oleh `poetry run pytest` sungguhan (`poetry-logs_v1_13_0.txt`,
project knowledge), bukan oleh review manual thread ini sendiri**: 1
failure nyata dari 1419 test, di luar seluruh test yang sudah
diidentifikasi dan diupdate untuk perubahan 55→59/4→0 di atas.
`TestInstrumentLoaderLayer2::test_is_deferred_property` meng-assert
`tin.is_deferred is True` — pencarian awal thread ini menemukan setiap
test yang mengandung angka HARDCODE (55, 59, 4, 695, 699) tapi melewatkan
test ini karena tidak mengandung angka apapun, hanya memilih TIN sebagai
contoh instrumen yang (dulu) deferred untuk menguji property `is_deferred`
itu sendiri secara langsung.

**Fix** (diverifikasi terhadap `InstrumentLoader` sungguhan SEBELUM
ditulis ke test file — bukan ditebak ulang, lihat dev-log untuk detail
reproduksi): `test_is_deferred_property` →
`test_is_deferred_property_false_for_active_instruments`
(`TestInstrumentLoaderLayer2`) — assertion dibalik ke `False` untuk TIN
dan COPPER (branch `is_deferred==True` sekarang dead terhadap config
real, karena nol instrumen Layer 2 yang deferred). Branch `True`
dipindah ke data sintetis — test BARU
`test_is_deferred_property_true_for_deferred_instrument`
(`TestInstrumentLoaderCoverageGaps`, kelas yang memang didesain untuk
"branch yang tidak dieksekusi config real") — pasangan
identity/taxonomy sintetis (`FAKE_DEFERRED`/`FAKE_ACTIVE`) diverifikasi
langsung terhadap `InstrumentLoader`/`merge_split_trees` sungguhan di
sandbox sebelum ditulis ke repo, menghasilkan `is_deferred` True/False
yang benar pada percobaan pertama.

**Pelajaran, dicatat agar tidak terulang**: pencarian berbasis grep
angka-hardcode (55/59/4/695/699) tidak cukup untuk menemukan test yang
menguji suatu MEKANISME (di sini: property `is_deferred`) lewat SATU
CONTOH SIMBOL spesifik tanpa angka apapun di assertion-nya. Real
`poetry run pytest` run adalah satu-satunya sumber kebenaran lengkap —
sesuai standing principle proyek ini ("never trust documentation without
empirical re-verification"), yang sama berlaku untuk pekerjaan sesi ini
sendiri, bukan hanya dokumen dari sesi sebelumnya.

### Verifikasi

**Dilakukan sesi ini (Claude, sebelum pytest sungguhan)**: setiap file
yang diedit dibaca lengkap sebelum diedit; `validate_instruments.py`
dibaca untuk mengonfirmasi constraint `proxy_for`/`proxy_correlation_expected`
SEBELUM memutuskan tidak menyertakan keduanya; kedua JSON Schema dibaca
untuk mengonfirmasi field baru valid; `InstrumentLoader` SUNGGUHAN
dijalankan (bukan cuma dibaca) terhadap kedua file config yang sudah
diedit, mengonfirmasi `count_total()=699`, `count_context()=59`,
`deferred_count()=0` sebelum diklaim di test manapun; assertion beberapa
test yang ditulis ulang dijalankan langsung terhadap `InstrumentLoader`
sungguhan itu.

**Dijalankan sesi ini oleh Ovi (`poetry-logs_v1_13_0.txt`)**: `poetry
lock` (24.5s, sukses) — tvdatafeed benar-benar terhapus dari lockfile.
`poetry run pytest tests/ -q` — **1 failed, 1418 passed** pada percobaan
pertama (lihat "Correction" di atas untuk fix-nya — belum di-re-run
empiris pasca-fix). `poetry run pytest tests/ --cov=src
--cov-fail-under=80 -q` — **81.41%** (di atas gate 80% dengan margin
nyaman; 1 kegagalan sama, tidak terkait coverage).
`python scripts/validate_instruments.py` — **PASSED**: "699 symbols
(Layer 1=640, Layer 2=59), no errors." `python scripts/check_glob_scope.py`
(Gate G-8) — **PASSED**: "0 glob-scope violations in src/."

**Belum dijalankan ulang pasca-correction**: `poetry run pytest` dengan
fix `test_is_deferred_property_*` di atas belum di-re-run empiris oleh
siapapun — arithmetic (1418 + 1 fix + 1 test baru = 1420 passed
diharapkan) belum dikonfirmasi eksekusi nyata. Rekomendasi: jalankan
ulang `poetry run pytest tests/ -q` sekali lagi untuk konfirmasi final
sebelum menganggap package ini benar-benar selesai.

---

## v1.12.1 — Decision C: Coverage Tranche (7/7 files) — Tiga Bug Nyata Ditemukan & Diperbaiki; pydantic Dihapus (Juli 2026)

Dokumen referensi: `GMI_Decision_Document_v5.docx` §3 (Decision C — coverage
tranche, sequencing dan exclusion policy) dan §9.2 Development Log
(pydantic removal, gated pada konfirmasi ulang tidak ada penggunaan lain).
Thread ini dimulai dari fresh clone di atas package v1.12.0 (dikonfirmasi
ulang terhadap live main commit `9f7eab3`: 1329 passed, coverage 70.36%,
699 instrumen — exact match, tidak ada drift), lalu menerapkan seluruh 7
file tranche Decision C secara berurutan sesuai sequencing yang sudah
diputuskan (`gold/mtf_alignment.py` → `gold/screener.py` →
`bronze/fred_ingester.py` → `bronze/bls_ingester.py` →
`bronze/imf_ingester.py` → `bronze/eia_ingester.py` →
`utils/pipeline_dashboard.py`). `correlation_matrix.py` dan
`hmm_regime.py` dikecualikan sesuai keputusan yang sudah ada (REPLACED by
design, tetap di denominator coverage, tidak disentuh).

Total: **140 test baru** (7 file baru: `test_screener.py`,
`test_fred_ingester.py`, `test_bls_ingester.py`, `test_imf_ingester.py`,
`test_eia_ingester.py`, `test_pipeline_dashboard.py`, plus 15 test
ditambahkan ke `test_mtf_alignment.py` yang sudah ada) | **1469 passed / 0
failed / 0 error** (Δ +140 dari v1.12.0 — 1329) | **coverage 70.36% →
81.97%** | **3 bug nyata ditemukan dan diperbaiki** (RISK-12, RISK-13,
RISK-14 — lihat `KNOWN_RISKS.md`) selama membangun fixture real untuk
fungsi yang sebelumnya nol coverage | **4 hardcode path dipromosikan ke
module-level constant** | **pydantic dihapus** (dikonfirmasi unused kedua
kalinya, `poetry remove pydantic`). PATCH bump (1.12.0 → 1.12.1): seluruh
perubahan adalah test coverage, bug fix pada path yang sebelumnya rusak,
dan dependency cleanup — tidak ada perubahan interface contract atau
schema Silver/Gold.

| File | Sebelum | Sesudah |
| --- | --- | --- |
| `gold/mtf_alignment.py` | 20% | 98% |
| `gold/screener.py` | 31% | **100%** |
| `bronze/fred_ingester.py` | 31% | 87% |
| `bronze/bls_ingester.py` | 28% | 94% |
| `bronze/imf_ingester.py` | 27% | 95% |
| `bronze/eia_ingester.py` | 24% | 95% |
| `utils/pipeline_dashboard.py` | 29% | 99% |

### FIX GLD-SCR-001 [src/gold/screener.py] — CROSS JOIN Regime Kosong Menghapus Seluruh Watchlist

**Root cause**: `build_watchlist()` mem-broadcast satu baris regime aktif
ke setiap kandidat MTF via `CROSS JOIN (SELECT * FROM regime_tbl LIMIT 1)
r`. Ketika `regime_store.parquet` belum ada, atau ada tapi tidak punya
baris untuk `run_date` yang tepat (`--force` run sebelum `gold_regime`,
atau backfill date yang belum pernah di-cover regime detection),
`regime_tbl` adalah placeholder nol-baris yang legitimate — dan CROSS JOIN
(Cartesian product) terhadap relasi kosong adalah kosong, definisi
matematis, terlepas dari berapa banyak baris di sisi lain. Efeknya:
seluruh watchlist hilang diam-diam meski data MTF/sector/active sempurna
valid.

**Opsi yang dipertimbangkan**: dipertahankan CROSS JOIN + guard manual
sebelum query (tambah kompleksitas kontrol-alur untuk masalah yang solvable
di level join) — ditolak. LEFT JOIN ... ON TRUE (dipilih) — pola
graceful-degrade yang PERSIS sama dengan yang sudah dipakai `sector_tbl`/
`active_tbl` beberapa baris di atasnya di query yang sama.

**Fix**: `CROSS JOIN (SELECT * FROM regime_tbl LIMIT 1) r` → `LEFT JOIN
(SELECT * FROM regime_tbl LIMIT 1) r ON TRUE`. Kolom regime
(`regime`, `regime_composite`, `regime_confidence`, `regime_transition`,
`transition_alert`) sekarang NULL — bukan menghapus baris — saat data
regime tidak tersedia, konsisten dengan filosofi "data field, bukan
keputusan" (GD §0.3).

**Diverifikasi empiris**: standalone DuckDB repro sebelum menyentuh source
— 0 baris keluar dengan CROSS JOIN, 2/2 baris terjaga (dengan `r.*` NULL
yang benar) dengan LEFT JOIN ON TRUE.

**Test baru**: `TestBuildWatchlistRegimeJoinRegression` (6 test) —
`tests/unit/test_screener.py`.

### FIX GLD-SCR-003 [src/gold/screener.py] — Correlation Concentration Guard Tidak Pernah Benar-Benar Jalan

**Root cause**: `_deduplicate_by_cluster()` memakai `pl.int_ranges(pl.len
()).over("cluster_id")` — bentuk JAMAK yang menghasilkan satu nilai
`List[Int64]` yang di-broadcast identik ke setiap baris dalam satu grup
(mis. `[0,1,2]`), BUKAN nomor urut per-baris. `filter(cluster_rank <
MAX_PER_CLUSTER)` selanjutnya raise `SchemaError` (perbandingan `<`
terhadap kolom List) pada SETIAP invocation nyata dengan data korelasi
asli — ditangkap diam-diam oleh `except Exception: logger.debug(...)` milik
fungsi ini sendiri. Efeknya: GD §15.1 Correlation Concentration Guard ("Max
2 posisi per correlation cluster") tidak pernah benar-benar tereksekusi
untuk input korelasi nyata manapun sejak fungsi ini ditulis.

**Fix**: `pl.int_ranges(pl.len())` → `pl.int_range(pl.len())` (bentuk
TUNGGAL — posisi skalar per-baris dalam grup, bukan list). Urutan baris
existing (`ORDER BY ABS(mtf_score) DESC, ...` dari caller) tetap terjaga
oleh `.over()`, jadi rank 0 dalam satu cluster selalu kandidat
prioritas-tertinggi yang sudah ada di cluster itu.

**Diverifikasi empiris**: standalone repro sebelum fix — plural raise
`SchemaError`; singular menghasilkan `Int64` per-baris yang benar (`[0,1,2]`
untuk grup 3-anggota, `[0,1]` untuk grup 2-anggota), filter selanjutnya
sukses.

**Test baru**: `TestClusterDeduplication` (3 test).

### FIX GLD-SCR-002 [src/gold/screener.py] — Dua Path Hardcode Dipromosikan ke Module Constant

`active_sym_path` (di `build_watchlist()`) dan `sentiment_path` (di
`_enrich_sentiment()`) sebelumnya dibangun inline sebagai f-string/`Path()`
literal, tidak bisa di-monkeypatch untuk isolasi test. Dipromosikan ke
`SILVER_ACTIVE_SYMBOLS_ROOT` dan `SILVER_SENTIMENT_ROOT` — nilai default
identik, filename per-`run_date` tetap dibangun dinamis di titik pemakaian.

### FIX GLD-MTF-COV-01 [src/gold/mtf_alignment.py] — Regime Path Hardcode + Coverage 20% → 98%

`_apply_regime_compatible()` sebelumnya hanya diuji lewat salinan
tangan-duplikasi dari logikanya sendiri (`_apply_mock` di test lama) —
fungsi asli, `_compute_mtf_alignment()`, dan `run()` tidak pernah benar-
benar dipanggil oleh test manapun. `regime_path` (string literal inline)
dipromosikan ke `REGIME_STORE_PATH` (module constant, pola sama dengan
`GOLD_SIGNALS_PATH`/`GOLD_MTF_PATH` yang sudah ada, dan `REGIME_STORE_PATH`
milik `macro_regime.py` sendiri) untuk memungkinkan isolasi test.

**Observasi (di-flag, TIDAK diperbaiki — di luar scope tranche test-only
ini)**: `reward_risk_ratio = (1.5*atr)/(1.25*atr)` secara aljabar selalu
sama dengan konstanta 1.2 untuk atr berapapun > 0 — kolom ini saat ini
tidak membawa informasi spesifik-simbol/volatilitas meski namanya begitu.
Test baru mengunci PERILAKU SAAT INI sebagai regression guard, bukan
endorsement bahwa formulanya benar. Keputusan ada di tangan Ovi.

**Test baru**: 22 test baru + 1 ditambahkan (`TestComputeMtfAlignment*`,
`TestApplyRegimeCompatibleReal`, `TestRunIntegration`,
`TestGetMtfSummaryFullPath`) di `test_mtf_alignment.py`.

### FIX EIA-5 [src/bronze/eia_ingester.py] — Cache Incremental-Fetch Tidak Pernah Ke-populate

**Root cause**: `_build_last_known_cache()` men-scan literal hardcode
`"data/bronze/commodity/eia/**/*.parquet"` — independen dari
`self.BASE_PATH` sepenuhnya, dan menunjuk ke `commodity/eia/`. Padahal
`write_macro(source="eia", domain="crude_oil")` milik ingester ini sendiri
menulis ke `BASE_PATH/macro/eia/crude_oil/`. Pattern scan itu TIDAK PERNAH
match file manapun yang pernah ditulis ingester ini. `FIX EIA-4`
(dokumentasi in-code dari fix sebelumnya) memperbaiki cara cache DIBACA
(key mismatch `spec['name']` vs `spec['id']`) — tapi cache-nya sendiri
tidak pernah terisi sejak awal, jadi fix itu saja tidak cukup memulihkan
perilaku yang dimaksud. Efeknya: `EIAIngester.run()` diam-diam selalu
pakai lookback 5-tahun penuh di SETIAP invocation, tidak pernah buffer
incremental 14-hari yang dimaksud.

**Fix**: `pattern` sekarang `str(self.BASE_PATH / "macro" / "eia" /
"crude_oil" / "**" / "*.parquet")` — BASE_PATH-relative (testable, benar
untuk deployment manapun) dan menunjuk ke domain yang benar-benar dipakai
`write_macro()`.

**Test baru**: `TestBuildLastKnownCache`, `TestIncrementalFetchWindow` —
keduanya gagal terhadap kode pre-fix (cache kosong, `KeyError`, window
incremental jatuh ke default 5-tahun), lulus terhadap fix.

### REMOVED [pyproject.toml, poetry.lock] — pydantic

Dikonfirmasi ulang unused (`grep` kedua kali, exit 1 — nol match di
`src/` maupun `scripts/`), perlakuan sama seperti alpha-vantage sebelum
benar-benar di-drop (Decision A / Checkpoint v5). `poetry remove
pydantic` — `pyproject.toml` dan `poetry.lock` diperbarui konsisten.
Keputusan "removal" ini adalah konfirmasi ulang eksplisit yang memang
diminta Development Log §9.2 ("belongs to whoever confirms there's no
other intended use"), bukan penghapusan diam-diam yang dilipat ke
perubahan lain.

### ADD [tests/unit/test_fred_ingester.py, test_bls_ingester.py, test_imf_ingester.py, test_pipeline_dashboard.py] — Coverage Tranche Sisanya

Empat file lain di tranche Decision C — tidak ada bug nyata ditemukan
(berbeda dari `screener.py`/`eia_ingester.py` di atas), murni penambahan
coverage terhadap fungsi yang sebelumnya nol test: parsing period BLS
(M/Q/A/M13), fallback FRED-mirror BLS saat `BLS_API_KEY` absen, batching
25-series BLS, proxy release-date IMF WEO (Oktober tahun berjalan → April
tahun depan → fallback run_date), clamping release_date FRED terhadap
run_date, gerbang SchemaValidator nyata (bukan mock) di keempatnya, dan
seluruh 5 section dashboard (`_section_job_status` s/d
`_section_data_freshness`) via `monkeypatch.chdir(tmp_path)` untuk
isolasi CWD-relative glob.

**Observasi (di-flag, tidak diperbaiki)**: `pipeline_dashboard.py`
membangun ~15 glob path sebagai literal CWD-relative tanpa module
constant — pola hardcode yang sama dengan yang diperbaiki di
`mtf_alignment.py`/`screener.py`, tapi file ini murni diagnostik
(kegagalan mode aman: dashboard menampilkan "no data", bukan korupsi
data) sehingga refactor besar-besaran path-nya sengaja tidak dilakukan
di tranche ini — prioritas terendah dari 7 file per sequencing Decision
C sendiri.

### Diverifikasi

Full suite (working copy): 1469 passed, 0 failed, 0 error. Full suite
(fresh independent clone kedua + venv terpisah + `poetry install --with
dev` terpisah): identik. Coverage: 81.97%, Gate G-6 lulus dengan margin
besar. Gate G-1 (162 file, 0 error), G-2 (0 f-string SQL), G-3 (699
simbol, exit 0), G-8 (0 glob-scope violation) — semua di-re-run manual,
semua PASS.

Package: `alpha-factory-v1_12_1-changed-files.zip` (`MANIFEST.md` +
`CHANGES.diff` + seluruh file baru/dimodifikasi). Base: v1.12.0 package
(itu sendiri belum pernah di-apply ke live main — lihat `MANIFEST.md`
package ini untuk urutan apply yang benar: v1.12.0 dulu, baru v1.12.1
di atasnya, ATAU gunakan package ini langsung di atas commit `9f7eab3`
karena isinya sudah kumulatif).

## v1.12.0 — Decision B: instruments.yaml Split + JSON Schema Layer; Decision D: Gate G-6 Trigger Fix (Juli 2026)

Dokumen referensi: `GMI_Decision_Document_v5.docx` §2 (Decision B Steps 2-3,
file-split mechanics + schema mechanism) dan §4 (Decision D, Gate G-6
trigger). Kedua keputusan sudah "decided, nothing implemented" di v5 —
thread ini adalah implementasi pass-nya, dimulai dari fresh clone
(dikonfirmasi ulang: commit `9f7eab3` di atas `ac3daaa` di atas `0048382`,
v1.11.2, 1300 passed, coverage 69.65% — exact match ke akun Decision
Document v5, tidak ada drift yang ditemukan).

Total: **instruments.yaml (1629 baris) dipecah menjadi 3 file** + **jsonschema
layer baru** (3 schema, 1 dependency dipromosikan dari transitive) +
**Gate G-6 CI trigger diperbaiki** + **29 test baru** | **1329 passed / 0
failed / 0 error** (Δ +29 dari v1.11.2 — 1300) | **coverage 69.65% → 70.36%
— Gate G-6 LULUS untuk pertama kalinya sejak gate ini ada**, dicapai lewat
coverage bertarget pada branch `instrument_loader.py` yang baru relevan
akibat perubahan constructor, bukan lewat penurunan threshold atau
pengecualian file. MINOR bump (1.11.2 → 1.12.0): shape on-disk
`config/instruments.yaml` berubah (file itu tidak ada lagi), tapi
`InstrumentLoader`'s API publik dan `Instrument` dataclass — kontrak yang
sebenarnya dikonsumsi 17 modul lain — tidak berubah sama sekali.

### ADR-027 [config/instruments_identity.yaml, config/instruments_taxonomy.yaml, src/config/yaml_split_merge.py] — Field-Classification Table untuk Decision B Split

**Status**: FINAL. **Decision**: `config/instruments.yaml` dipecah menjadi
`instruments_identity.yaml` (sourcing/identitas: `symbol`, `raw_symbol`,
`yfinance_symbol`, `tvfeed_symbol`, `timezone`, `base_currency`,
`proxy_instrument`) dan `instruments_taxonomy.yaml` (klasifikasi/routing/
skor: `layer`, `context_category`, `context_group`, `context_available`,
`include_in_forecast`, `proxy_for`, `proxy_correlation_expected`,
`reclassified_from`, `deferred_reason`, `planned_wave`, `reliability_flag`,
`exclude_from_lead_lag_leader`, `commodity_role`, `commodity_subcategory`,
`requires_fx_normalization`, `notes`, `structural_break`, plus seluruh
blok `_meta`). Join positional/structural (path + index list sama = satu
instrumen) — BUKAN flat-list-plus-key-eksplisit, sesuai keputusan Decision
Document v5 §2.1. `symbol` sengaja diulang di kedua file sebagai anchor
self-checking, bukan pelanggaran field-disjointness.

**Context**: tabel klasifikasi field TIDAK ditentukan field-by-field di
dokumen manapun sebelum thread ini — Decision Document v5 menetapkan
STRATEGI (positional join, dua file per concern) tapi bukan pemetaan
per-field. Tabel di atas diturunkan empiris dari
`_CONTEXT_CONSUMED_KEYS`/`Instrument` dataclass yang live di
`instrument_loader.py` dan grep lengkap terhadap setiap key yang benar-benar
dipakai di file real (23 key unik ditemukan, bukan superset spekulatif dari
dokumen arsitektur) — bukan dari asumsi dokumen.

**Rationale**: setiap field diklasifikasikan dengan pertanyaan tunggal:
"apakah ini menjawab APA/DI MANA mengambil data (identity), atau BAGAIMANA
mengklasifikasikan/merutekannya (taxonomy)?" `proxy_instrument` (simbol mana
yang dipakai) masuk identity meski berdekatan konsep dengan `proxy_for`
(benchmark apa yang direpresentasikan, taxonomy) dan
`proxy_correlation_expected` (metadata kualitas, taxonomy) — garis batasnya
konsisten dengan `tvfeed_symbol`/`yfinance_symbol` yang juga soal "di mana
ambil data". `notes` diklasifikasikan taxonomy (audit-trail/rationale,
sejenis dengan `reclassified_from`/`deferred_reason`), bukan identity.

**Verifikasi**: setiap keputusan split diverifikasi lewat reconstruction
diff — `merge_split_trees(identity, taxonomy)` terhadap kedua file hasil
split harus SAMA PERSIS (dict equality, bukan visual spot-check) dengan
`instruments.yaml` v1.5 asli sebelum split dijalankan. Dijalankan dua kali:
sekali sesaat setelah split structural, sekali lagi setelah 15 blok komentar
ADR-rationale asli dikembalikan ke `instruments_taxonomy.yaml` (100% dari
15 blok itu anchor ke field taxonomy-side — tidak ada yang perlu dipecah
lintas file). Kedua kali: identik.

**Consequences**: `InstrumentLoader.__init__` sekarang baca dan merge dua
file — `_load_layer1()`/`_load_layer2()`/`_load_subcategory_meta()`/semua
`_build_*()` TIDAK BERUBAH SAMA SEKALI (menerima dict merged yang identik
dengan yang dulu di-parse dari satu file). `YAML_PATH` dihapus (bukan
alias) — grep sebelum perubahan mengkonfirmasi tidak ada caller manapun
yang pernah pass `yaml_path=` custom.

**Rejected**: flat-list-plus-explicit-symbol-key join (Decision Document
v5 sudah menolak ini — menambah failure mode baru: rename di satu file
tanpa mirror di file lain, silent). Horizontal-by-layer split (2 file:
layer1.yaml + layer2.yaml) — tidak menyelesaikan debt aslinya ("empat
concern satu file"), garis batasnya di sumbu yang salah.

### ADD [config/regime_sector_weights.yaml, src/gold/sector_rotation.py] — REGIME_SECTOR_WEIGHTS Dieksternalisasi

Dict literal 147-baris di `sector_rotation.py` (5 regime × 20 key)
dipindah ke `config/regime_sector_weights.yaml`, dimuat via
`yaml.safe_load()` saat module import ke variabel bernama sama
(`REGIME_SECTOR_WEIGHTS`, tipe `dict`) — `test_sector_rotation.py` yang
mengimpor nama ini langsung TIDAK PERLU DIUBAH (17 test, 0 perubahan).
Nilai diekstrak via `ast.literal_eval()` terhadap module live, bukan
ditranskrip ulang manual — menghindari kelas bug yang sama persis dengan
RISK-10 (`commodity_precious_metals` vs `commodity_precious`, v1.11.0).
Diverifikasi: dict hasil load YAML `==` dict literal Python asli
(`assert` langsung, bukan spot-check per-key).

### ADD [config/schemas/instruments/identity.schema.yaml, taxonomy.schema.yaml, regime_sector_weights.schema.yaml] — JSON Schema Layer (Decision B Step 3)

Library `jsonschema` (sudah resolve transitive sejak v1.11.2, nol import
langsung sebelum ini) dipromosikan ke direct dependency eksplisit di
`pyproject.toml`. Tiga schema Draft-7, ditulis YAML mengikuti konvensi
Bronze schema registry yang sudah ada (`config/schemas/*.yaml`, 13 file).
`InstrumentLoader` sendiri TIDAK menyentuh `jsonschema` — independensinya
dari `validate_instruments.py` dipertahankan (header
`validate_instruments.py` sendiri menyatakan independensi ini disengaja,
membela Gate G-3).

**Ditemukan empiris saat pengujian pertama terhadap data real** (bukan
disengaja/direncanakan): schema awal untuk `yfinance_symbol` terlalu
ketat (`type: string`) — real data punya `yfinance_symbol: null` eksplisit
untuk CPO/RUBBER/TIN (tvdatafeed-only, deferred Wave 2), by design bukan
data hilang. Schema diperbaiki (`type: ["string", "null"]`) setelah
verifikasi langsung ke data. Dikonfirmasi schema punya "gigi" sungguhan:
uji korupsi sengaja (`context_available` sebagai string `"true"` alih-alih
boolean; `commodity_subcategory` dengan enum tidak valid) — keduanya
tertangkap setelah `context` diberi schema struktural penuh (draft pertama
memakai `additionalProperties: true` untuk `context` yang ternyata membuat
1 dari 2 korupsi lolos tanpa terdeteksi — diperbaiki dengan `$defs`
recursive sebelum dianggap selesai).

Cakupan schema dibatasi sengaja ke structural/type/enum check — invariant
lintas-file (jumlah total 699, weight sum = 1.00, symbol lockstep antar
file) TETAP hand-written di `validate_instruments.py`, persis sesuai
keputusan Decision Document v5 §2.2 ("a schema was never going to express
these cleanly").

### UPD [scripts/validate_instruments.py] — validate_data() / validate() / validate_split()

`validate()` lama dipecah: `validate_data(data, extra_errors=None)` berisi
seluruh rule set hand-written (tidak berubah logic-nya), `validate(path)`
dipertahankan APA ADANYA sebagai entry point single-combined-file legacy
(~40 fixture sintetis di `test_validate_instruments.py` — ZERO perubahan),
`validate_split(identity_path=None, taxonomy_path=None)` adalah entry
point produksi real baru: jsonschema-check kedua file split secara
terpisah, merge positional via `yaml_split_merge` (implementasi yang SAMA
dipakai `InstrumentLoader` — bukan dua yang bisa diam-diam berbeda), lalu
`validate_data()` pada hasil merge. `if __name__ == "__main__":` sekarang
memanggil `validate_split()` — invocation CI Gate G-3
(`poetry run python scripts/validate_instruments.py`) tidak perlu berubah
sama sekali, hanya perilaku internalnya. `merge_split_trees()` yang raise
`ValueError` pada misalignment struktural SENGAJA tidak ditangkap di
`validate_split()` — file yang tidak selaras bukan temuan validasi biasa
untuk dilaporkan dan dilanjutkan, itu berarti kedua file bukan pasangan
yang koheren sama sekali.

### FIX [tests/unit/test_validate_instruments.py, tests/unit/test_archived_migration_scripts.py, src/config/pipeline_config.py] — Referensi ke config/instruments.yaml Lama

4 titik di `test_validate_instruments.py` yang point langsung ke
`"config/instruments.yaml"` (file yang sekarang tidak ada) diperbaiki ke
`validate_split()` / merge dua file split — 2 di antaranya adalah
reproduksi audit real (`test_real_file_all_14_commodity_instruments_...`,
`test_real_file_all_domain_scores_sum_to_one`), harus tetap membaca data
real, bukan disintesis. `test_archived_migration_scripts.py`'s
`INSTRUMENTS_YAML` constant (RISK-11 guard) diperluas jadi
`GUARDED_CONFIG_FILES` — memeriksa ketiga file config baru, bukan satu,
karena split menambah permukaan yang perlu dijaga dari
migrate_instruments.py/build_instruments_v14.py yang diarsipkan.
`pipeline_config.py`'s field `instruments_yaml` (Path constant yang
grep-dikonfirmasi tidak pernah benar-benar dibaca `InstrumentLoader` —
murni dokumentasi) diganti `instruments_identity_yaml` +
`instruments_taxonomy_yaml` supaya tidak jadi referensi basi ke file yang
sudah dihapus.

### UPD [.github/workflows/ci.yml] — Decision D: Gate G-6 Trigger

`if: github.event_name == 'pull_request'` → juga fire di
`push` ke `main`. Dikonfirmasi via GitHub Actions API (Decision Document
v5) bahwa G-6 tidak pernah benar-benar jalan di 3 commit live manapun —
repo ini tidak punya langkah PR (tidak ada push access, model zip-apply),
jadi kondisi PR-only secara struktural tidak pernah tercapai meski tabel
Gate Hierarchy di CI/CD Ops Guide v1.7.4 menyatakan "PR only | Ya |
blocking". Sekarang angka coverage sungguhan tampil di setiap landing
nyata di `main`.

### ADD — 29 Test Baru

`tests/unit/test_yaml_split_merge.py` (17, BARU) — util merge diuji
isolasi dengan fixture sintetis kecil: happy path, deteksi korupsi
(anchor mismatch, length mismatch, field overlap, conflicting scalar —
semua `raise ValueError` dengan pesan jelas), None-handling, non-mutating
input. `TestValidateSplit` + `TestJsonSchemaLayer` (6, di
`test_validate_instruments.py`) — `validate_split()` dengan path eksplisit,
propagasi `ValueError` pada file yang tidak selaras (bukan silent-fail),
2 uji korupsi jsonschema. `TestInstrumentLoaderCoverageGaps` (6, di
`test_instrument_loader.py`) — properti `is_idx`/`is_forex`/`hive_key`,
`get()` dengan `market=` yang tidak match, `by_sector()`, `symbol_list()`
dengan filter, `_build_index()` (dead code terhadap data real sejak
ADR-003 — SPX/VIX/DXY sudah direklasifikasi keluar dari `index:` Layer 1;
dites lewat fixture sintetis via constructor override yang baru), dan
guard defensif `isinstance()` di `_load_layer2()`/`_load_subcategory_meta()`
untuk blok context yang malformed.

### UPD [pyproject.toml] — Version + Dependency

`jsonschema = ">=4.0"` dipromosikan explicit direct dependency (dulu
resolve transitive saja, nol import). `pydantic` DIFLAG (bukan dihapus)
— treatment yang sama dengan alpha-vantage sebelum benar-benar di-drop
(Decision A / Checkpoint v5): masih nol usage di `src/`/`scripts/`
(dikonfirmasi grep ulang), penghapusan sengaja tidak dibundel ke
perubahan schema yang tidak berhubungan — keputusan itu milik siapapun
yang mengkonfirmasi tidak ada penggunaan lain yang direncanakan, bukan
sesuatu untuk diputuskan sepihak di sini. Version `1.11.2` → `1.12.0`.
`poetry lock` + `poetry install --with dev` dijalankan ulang, resolve
bersih.

### Diverifikasi

- Full suite (`poetry run pytest tests/ -q`): **1329 passed, 0 failed, 0
  error** (working copy dan independent fresh extraction — lihat MANIFEST).
- Coverage (`--cov=src --cov-fail-under=70`): **70.36%** — Gate G-6 LULUS,
  pertama kali sejak gate ini eksis di CI. `src/config/yaml_split_merge.py`
  baru: 100%. `src/gold/sector_rotation.py`: 100%. `src/config/
  pipeline_config.py`: 100%. `src/config/instrument_loader.py`: 93%
  (naik dari cakupan sebelumnya lewat 6 test baru bertarget).
- Gate G-3 (`python scripts/validate_instruments.py`, invocation identik
  CI): `VALIDATION PASSED — 699 symbols (Layer 1=640, Layer 2=59), no
  errors.`
- Reconstruction diff (`merge_split_trees(identity, taxonomy) ==
  instruments.yaml` asli): identik, dijalankan sebelum dan sesudah
  reinsersi 15 blok komentar.
- Jsonschema teruji punya gigi sungguhan terhadap data korup sintetis
  (bukan vacuously permissive) sebelum dianggap selesai.
- `tests/COUNT_BASELINE.txt`: `1300` → `1329`.



Dokumen referensi: `GMI_Decision_Document_v3.docx` §4 Next Steps (item "Decision
B step 1... Coverage gap... Archive stale migration scripts" — carried
forward via `GMI_Implementation_Checkpoint_v6.docx` §8 sebagai priority
queue untuk thread berikutnya), dijalankan sebagai satu pass empirik
sebelum GMI Wave 1 Cycle 4 dimulai.

Total: **1 gap ditutup penuh** (coverage 0% pada 3 file Bronze) + **1 risk
serius ditemukan dan diperbaiki** (dua script migrasi destruktif, RISK-11)
+ **2 hardcode/robustness fix kecil** (throttle constant duplikat, TVS-2
backoff gap) | **1300 passed / 0 failed / 0 error** (Δ +86 dari v1.11.1 —
1214). Tidak ada perubahan schema, tidak ada perubahan API publik — semua
PATCH-level.

Konteks: repository live main dikonfirmasi masih di commit `0048382`
(v1.11.0) via `git ls-remote` — ADR-026 belum pernah di-push (sesuai
peringatan eksplisit Checkpoint v6 §6). Thread ini dimulai dari fresh
clone, menerapkan diff ADR-026 yang sudah diverifikasi (paket
`adr-026-changed-files_v1_11_0-to-v1_11_1.zip`, 1214 passed dikonfirmasi
ulang), lalu melanjutkan ke item-item completion-gap di atasnya.

### ADD [tests/unit/test_treasury_ingester.py] — Coverage 0% → 100%

`src/bronze/treasury_ingester.py` (27 statements) — delegate tipis ke
`FREDIngester` untuk yield curve harian (input utama macro regime GD
§8.1). 12 test baru: early-exit tanpa `FRED_API_KEY`, delegasi dengan
`series_filter` yang benar, exception dari delegate ditangkap bukan
di-propagate, serta invariant arsitektural FIX TI-1 (tidak inherit
`BronzeIngester` — tidak pernah menulis Bronze secara langsung, GD §17.3).

### ADD [tests/unit/test_tvdatafeed_session.py] — Coverage 0% → 96%

`src/bronze/tvdatafeed_session.py` (99 statements) — session manager IDX
primer (IDD §6). 35 test baru mencakup singleton lifecycle, cooldown FIX
TVS-1, retry/backoff, dan health-check gating. Empat baris tersisa yang
tidak tercover (44-47) adalah `try/except ImportError` untuk `tvDatafeed`
itu sendiri — genuinely sulit ditest secara meaningful tanpa memanipulasi
`sys.modules` sebelum import pertama; diterima sebagai gap kecil yang
wajar untuk optional-dependency shim.

### FIX TVS-2 [src/bronze/tvdatafeed_session.py] — Health-check-failure branch tidak punya backoff

**Ditemukan saat menulis test, bukan dari membaca ulang docstring** — pola
yang sama persis dengan `commodity_precious` (v1.11.0) dan `--pre`
argparse (v1.11.1): kedua bug sebelumnya juga ditemukan lewat eksekusi,
bukan review dokumen. `_connect()`'s exception branch sudah punya
`time.sleep(RETRY_BACKOFF_BASE ** attempt)`, tapi branch health-check-gagal
(login sukses, tapi fetch 1-bar kesehatan gagal) jatuh ke iterasi
berikutnya tanpa sleep sama sekali — tiga percobaan `TvDatafeed()`
beruntun tanpa delay setiap kali login sukses tapi health check gagal.
Diperbaiki: branch tersebut sekarang memanggil backoff yang sama.
Regression guard:
`test_tvdatafeed_session.py::TestConnect::test_health_check_failure_backs_off_between_attempts`.

### ADD [tests/unit/test_tvdatafeed_adapter.py] — Coverage 0% → 100%

`src/bronze/tvdatafeed_adapter.py` (60 statements) — `SourceAdapter` IDX
primer (GD §3.5). 28 test baru: kontrak `SourceAdapter`, FIX TVA-1
(`_null_count` instance-level, bukan class-level — dua instance tidak
boleh berbagi state kegagalan), semua jalur `fetch()` (early-exit, empty
result + `force_reconnect()`, sukses + normalisasi kolom, exception
session/auth-keyword vs non-session), `_estimate_n_bars` termasuk FIX
TVA-3 (jam sesi IDX 5.5 jam/hari, bukan 8 jam US), dan `_check_null_alert`
boundary di `IDX_NULL_ALERT_THRESHOLD`.

### RISK-11 [scripts/migrate_instruments.py, scripts/build_instruments_v14.py] — Archive dua script migrasi destruktif — lihat KNOWN_RISKS.md

Ditemukan saat menilai kedua script untuk archival (Decision Document v3
Priority 3): **keduanya mengeksekusi write destruktif ke
`config/instruments.yaml` pada IMPORT TIME** — tidak ada
`if __name__ == "__main__":` guard di manapun, jadi bahkan `import` biasa
(bukan hanya eksekusi langsung) sudah cukup memicu overwrite. Root cause,
blast radius, dan verifikasi lengkap di `KNOWN_RISKS.md` RISK-11 — tidak
diduplikasi di sini.

Fix: kedua script dipindah (`git mv`, histori git terjaga) ke
`scripts/archive/` dengan guard `raise SystemExit(...)` tanpa syarat
sebagai baris pertama file (sebelum `sys.path.insert`, sebelum import
apapun). `src/config/instruments_raw.py` (data murni, satu-satunya
konsumen adalah `migrate_instruments.py` yang kini diarsipkan) ikut
dipindah ke `scripts/archive/instruments_raw.py` — sekaligus menghapusnya
dari cakupan `[tool.coverage.run] source = ["src"]`, tanpa perlu entry
`omit` baru. `Makefile`'s target `migrate` dipertahankan (tidak dihapus)
tapi sekarang gagal dengan pesan jelas menunjuk `scripts/archive/README.md`.
`README.md` project-structure tree dan header komentar
`scripts/validate_instruments.py` diperbarui. Test regression baru:
`tests/unit/test_archived_migration_scripts.py` (11 test, dijalankan di
subprocess terisolasi karena bug aslinya adalah side-effect saat import).

### FIX [src/bronze/market_ingester.py] — Magic number `0.6` diduplikasi di dua lokasi

`time.sleep(0.6)` muncul identik di Layer 1 loop dan Layer 2/context loop,
masing-masing dengan komentar terpisah menjelaskan alasan yang sama.
Diekstrak menjadi satu named constant `YFINANCE_THROTTLE_SECONDS = 0.6` —
menghilangkan risiko kedua lokasi diam-diam divergen jika rate limit
yfinance berubah di masa depan dan hanya satu lokasi yang diupdate. Tidak
ada test yang hardcode nilai literal `0.6` secara langsung (dikonfirmasi
via grep sebelum perubahan), jadi tidak ada test yang perlu diupdate.

### Coverage — 65.60% → 69.65% (+4.05pp)

Baseline pre-thread 65.60% dikonfirmasi ulang secara empiris (bukan
diasumsikan dari checkpoint sebelumnya) sebelum perubahan apapun. Setelah
menutup gap 0% pada tiga file di atas: **69.65%**, masih 0.35pp di bawah
gate CI 70%. File-file berikutnya dengan coverage rendah (`bls_ingester.py`
28%, `imf_ingester.py` 27%, `eia_ingester.py` 24%, `fred_ingester.py` 31%,
`mtf_alignment.py` 20%, `screener.py` 31%, `pipeline_dashboard.py` 29%)
**sengaja tidak disentuh** pada pass ini — semuanya lebih besar/kompleks
dari tiga file yang sudah diidentifikasi eksplisit sebagai starting point
oleh Checkpoint v6, dan memilih yang mana untuk dikerjakan berikutnya
adalah keputusan prioritas baru yang belum pernah dibuat di dokumen
manapun. Bukan diselesaikan diam-diam dengan pilihan ad-hoc.

### Diverifikasi

Full suite: **1300 passed / 0 failed / 0 error** — baik di working copy
maupun independent fresh extraction (own venv, own `poetry install --with
dev`, tidak ada shared state). Semua CI gate (G-1 syntax 154 file 0 error,
G-2 f-string SQL 0 violation, G-3 `validate_instruments.py` exit 0 — 699
symbols, G-8 glob-scope 0 violation) lulus. `tests/COUNT_BASELINE.txt`
diupdate 1214 → 1300.

## v1.11.1 — ADR-026: Poetry/Conda Environment Reuse Guard (Juli 2026)

Dokumen referensi: `ADR-026_poetry_conda_environment.md`

Total: **1 gap ditutup** (Poetry-conda env reuse tidak pernah diverifikasi
secara eksplisit) + **1 bug ditemukan dan diperbaiki di dalam ADR-026
sendiri** (script `--pre` flag tidak pernah di-declare di argparse,
padahal dipanggil eksplisit oleh Makefile pada ADR yang sama) | **1214
passed / 0 failed / 0 error** (Δ +10 dari v1.11.0 — 1204). Tooling/docs
only — tidak ada perubahan di `src/`, tidak ada perubahan CI.

Konteks: `ADR-026_poetry_conda_environment.md` berstatus **"decided, not
implemented"** pada saat penulisan. Setiap klaim dalam dokumen tersebut
diverifikasi ulang secara empiris terhadap live repository (fresh clone
dari `github.com/Ovi-xyz/alpha-factory`, commit `0048382`, single
squashed commit tanpa tag) sebelum diimplementasikan: commit history,
`pyproject.toml` version, absennya `poetry.toml`, `Makefile` yang
menjalankan `python`/`pytest` bare (tanpa prefix `poetry run`), dan kedua
bug `README.md` (conda branch berhenti di `conda activate` tanpa chain ke
`poetry install --with dev`; `poetry shell` dikonfirmasi mati di Poetry
2.4.1 — direproduksi langsung: *"Since Poetry (2.0.0), the shell command
is not installed by default"*).

Mekanisme inti ADR ini — bahwa Poetry secara default (`virtualenvs.create
= true`, tidak dimodifikasi di manapun di repo ini) sudah mendeteksi dan
reuse conda env aktif via `CONDA_PREFIX`, bukan membuat virtualenv
terpisah — diverifikasi langsung: `poetry env info --path` di dalam
`CONDA_PREFIX` yang disimulasikan mengembalikan path `CONDA_PREFIX`
tersebut, bukan cache dir Poetry sendiri.

### ADD ADR-026 [scripts/check_poetry_env.py] — Diagnostic pre/post check

Script baru, stdlib-only (`--pre` regex-parse `environment.yml`'s
`name:` field, bukan `import yaml`, karena `--pre` harus jalan SEBELUM
PyYAML terinstall). Dua mode:

- `--pre`: assert conda env dengan nama persis sesuai `environment.yml`
  sedang aktif (`CONDA_PREFIX` basename check).
- `--post`: assert `poetry env info --path` persis sama dengan
  `CONDA_PREFIX` aktif — mendeteksi jika Poetry diam-diam membuat
  virtualenv terpisah alih-alih reuse env yang aktif.

Diverifikasi empiris untuk seluruh state space: no env aktif (`--pre`
FAIL), env aktif dengan nama salah (`--pre` FAIL), env aktif dengan nama
benar (`--pre` PASS), Poetry reuse env aktif (`--post` PASS), dan skenario
kegagalan sebenarnya — tidak ada env aktif, Poetry fallback ke cache
dir-nya sendiri (`--post` FAIL dengan pesan yang tepat).

### ADD ADR-026 [poetry.toml] — Pin default eksplisit

`[virtualenvs] create = true` — tidak mengubah perilaku hari ini (Poetry
2.4.1 sudah default ke ini), murni asuransi terhadap Poetry mengubah
default ini di versi mayor berikutnya (`prefer-active-python` sendiri
baru menjadi default di 2.0).

### UPD ADR-026 [Makefile] — Wire guard ke setup/install, tambah target `doctor`

`check_poetry_env.py --pre` sebelum `poetry install --with dev`,
`--post` sesudahnya, di kedua target `setup` dan `install`. Target baru
`doctor` menjalankan keduanya tanpa reinstall — untuk verifikasi cepat
setelah `conda activate` di shell baru.

### FIX ADR-026 [scripts/check_poetry_env.py] — `--pre` tidak ter-declare di argparse

**Ditemukan secara empiris, bukan dari membaca ulang dokumen** — pola
yang sama dengan inkonsistensi `commodity_precious` vs
`commodity_precious_metals` yang ditemukan saat implementasi v1.11.0
(lihat entry di bawah). Spesifikasi script ADR-026 hanya men-declare
`--post` di argparse (`--pre` dimaksudkan sebagai default saat tidak ada
flag), tapi spesifikasi `Makefile` di ADR yang SAMA memanggil script
tersebut dengan `--pre` eksplisit. Menjalankan `make setup` pada
percobaan pertama akan gagal: `error: unrecognized arguments: --pre`.

Diperbaiki dengan men-declare `--pre` sebagai flag eksplisit
(`action="store_true"`) — perilaku tidak berubah (masih default ke
pre-check jika kedua flag absen), tapi sekarang self-documenting dan
valid dipanggil eksplisit dari `Makefile`.

### ADD [tests/unit/test_check_poetry_env.py] — 10 test baru

Mengikuti konvensi test `scripts/` yang sudah ada
(`test_check_glob_scope.py`, `test_preflight_scripts.py`): import modul
langsung via `sys.path` insertion, `monkeypatch` untuk `CONDA_PREFIX` dan
`subprocess.run`. Termasuk `TestArgparseSurface` — guard permanen untuk
kelas bug yang sama persis dengan FIX di atas (memanggil `main()` dengan
`sys.argv` yang mensimulasikan pemanggilan dari `Makefile`).

### Verifikasi

Full suite: **1214 passed, 0 failed** (baseline 1204 + 10 baru,
`tests/COUNT_BASELINE.txt` diperbarui). Gate G-1 (ast.parse, 150 file, 0
error), Gate G-2 (f-string SQL, 0 pelanggaran), Gate G-3
(`validate_instruments.py`, 699 symbols, exit 0), Gate G-8 (glob-scope, 0
pelanggaran) — semua PASS, tidak berubah dari v1.11.0. Coverage tetap
65.60% (pre-existing, `src/` tidak tersentuh perubahan ini — konfirmasi
independen bahwa scope perubahan murni tooling/docs).

Version bump: 1.11.0 → **1.11.1** (PATCH — tooling/docs fix, tidak ada
API atau schema change, per konvensi versioning proyek ini).

---

## v1.11.0 — GMI Decision Document v3 Implementation: Dependency Architecture, Security Hardening, Commodity Taxonomy (Juli 2026)

Dokumen referensi: `GMI_Decision_Document_v3.docx` (Decision A, Decision B
Step 1)

Total: **Decision A diimplementasikan penuh** (poetry sebagai single
source of truth dependency graph, CI fix, Gate G-8 + Python matrix
wiring) + **Decision B Step 1 diimplementasikan** (commodity taxonomy gap
closure — commodity_role/commodity_subcategory + REGIME_SECTOR_WEIGHTS
disaggregation) + **1 inkonsistensi internal ditemukan dan diperbaiki**
di Architecture v2.1 Addendum sendiri (commodity_precious vs
commodity_precious_metals) + **security hardening** (.gitignore
ditambahkan, runtime artifact di-untrack) | **1204 passed / 0 failed / 0
error** (Δ +16 dari v1.10.0 — 1188)

Konteks: GMI_Decision_Document_v3.docx menyatakan status **"decided,
nothing implemented"** pada saat penulisan, dan mengoreksi klaim
GMI_Implementation_Checkpoint_v4.docx yang ternyata tidak akurat — Gate
G-8 dan Python 3.11+3.12 matrix TIDAK pernah benar-benar ter-wire ke
ci.yml meskipun code pendukungnya (`scripts/check_glob_scope.py`,
`scripts/preflight/*.py`) sudah ter-commit. Setiap klaim dalam dokumen
ini diverifikasi ulang secara empiris terhadap live repository (fresh
clone dari `github.com/Ovi-xyz/alpha-factory`, tag v1.10.0) sebelum
diimplementasikan — bukan dipercaya dari checkpoint sebelumnya.

### Decision A [pyproject.toml, environment.yml, Makefile, .github/workflows/ci.yml, .env.example] — Poetry sebagai single source of truth dependency graph

**Blocking CI fix — tanpa ini, `--job all`/test suite tidak bisa jalan di
CI sama sekali.** Root cause diverifikasi langsung: `pip install -e
'.[dev]'` menghasilkan `WARNING: alpha-factory 1.10.0 does not provide
the extra 'dev'` karena pytest/pytest-cov hanya berada di
`[tool.poetry.group.dev.dependencies]`, tidak pernah diekspos lewat
`[tool.poetry.extras]` — pip sama sekali tidak melihatnya. `poetry.lock`
sudah di-generate sejak ADR-020 tapi tidak pernah benar-benar dibaca CI,
karena hanya Poetry CLI yang membacanya.

- `ci.yml`: `pip install -e '.[dev]'` -> `poetry install --with dev`.
  Diverifikasi: resolve bersih, pytest 9.1.1 + pytest-cov 7.1.0
  terinstall.
- `pyproject.toml`: tvdatafeed di-declare ulang sebagai Poetry git
  dependency (`{git = "https://github.com/rongardF/tvdatafeed.git"}`) —
  dikonfirmasi 404 di PyPI JSON API untuk versi apapun. Import
  diverifikasi langsung: `import tvDatafeed` berhasil.
- `environment.yml`: blok `pip:` dipensiunkan sepenuhnya — root cause
  kegagalan conda di `alpha-factory logs.txt` (`ERROR: Could not find a
  version that satisfies the requirement tvdatafeed>=2.0`) adalah conda
  me-resolve seluruh blok `pip:` sebagai SATU pip call atomik; satu spec
  rusak (tvdatafeed) meracuni instalasi PyYAML, pytest, dan semua paket
  lain di blok yang sama. `alpha-vantage` juga di-drop — diverifikasi
  tidak pernah di-import sebagai package pihak ketiga di manapun di
  `src/` (hanya `AlphaVantageForexAdapter` buatan sendiri, berbasis
  `requests`/`httpx`, yang eksis).
- **ADD Gate G-8**: wired ke `ci.yml` setelah Gate G-3 — code
  (`scripts/check_glob_scope.py`) sudah eksis sejak v1.10.0 tapi tidak
  pernah benar-benar terhubung ke workflow manapun (dikonfirmasi via
  `grep` langsung pada file live sebelum perubahan ini).
- **ADD Python 3.11 + 3.12 matrix**: `fail-fast: false`. Python floor
  TETAP `>=3.11,<3.13` (tidak dinaikkan) — matrix ini yang benar-benar
  menguji kedua ujung range yang sudah dideclare, bukan hanya 3.12 seperti
  sandbox yang menulis ADR-020.
- **ADD `.env.example`**: gap orisinal (belum pernah ada di commit
  manapun), spesifikasi lengkap sudah ada di IDD v1.0 §8.5. Isi
  direkonsiliasi terhadap seluruh `os.getenv()` call site nyata di `src/`
  via grep empiris.
- `Makefile`: `make install`/`make setup` di-repoint ke
  `poetry install --with dev` — daftar pip hardcoded sebelumnya sudah
  drift sendiri (masih `pandas-ta`, bukan `-classic`; tidak ada
  scipy/statsmodels/tvdatafeed).

### Security — `.gitignore` ditambahkan, runtime artifact di-untrack

Repo sebelumnya **tidak memiliki `.gitignore` sama sekali**. Audit
empiris (`git log --all --name-only --diff-filter=A` di ketiga commit)
mengonfirmasi **tidak ada secret/API key/credential** yang pernah
ter-commit di working tree maupun history manapun — tapi menemukan 5 file
`.DS_Store` dan 3 runtime artifact (`data/health/hmm_regime_model.pkl`,
`pipeline_runs.db`, `progress.db`) ter-tracked di git.

- `.gitignore` dibuat: secrets (`.env` + varian, `*.pem`/`*.key`),
  Python/Poetry cache, `data/` (runtime pipeline output — terlalu besar
  untuk version control per estimasi GD §7, ~73-117 GB, dan
  regenerable), OS/editor metadata.
- 5x `.DS_Store` + 3x `data/health/*` di-untrack via `git rm --cached`
  (bukan history rewrite — tidak diperlukan karena tidak ada secret yang
  pernah ter-commit). File tetap ada di disk lokal.
- Rasional keamanan untuk pickle binary di source control secara khusus:
  `pickle.load()`/`joblib.load()` bisa mengeksekusi kode arbitrary jika
  file pernah tertukar atau dimodifikasi pihak lain — risiko
  deserialization yang berdiri sendiri terlepas dari isi file saat ini.
  Empiris dikonfirmasi juga rentan drift versi: test suite run yang men-
  generate ulang `hmm_regime_model.pkl` dari awal menghilangkan
  `InconsistentVersionWarning` yang muncul saat file lama (dari
  environment sklearn berbeda) masih dipakai.

### Decision B Step 1 [config/instruments.yaml, src/config/instrument_loader.py, src/gold/sector_rotation.py, scripts/validate_instruments.py] — Commodity dual-classification gap closure

**Menutup gap Architecture v2.1 Addendum §7.1/§8** — `commodity_role`/
`commodity_subcategory` sudah dispesifikasikan turun ke level kode di
dokumen tersebut, tapi nol occurrence eksis di manapun di live repo
sebelum perubahan ini (dikonfirmasi via `grep` empiris). `sector_rotation.py`
masih memakai flat key `"commodity"` tunggal — komentarnya sendiri masih
mereferensikan IDD §4, bukan Addendum.

- `instruments.yaml`: `commodity_role` (`trading`/`context`) +
  `commodity_subcategory` (`energy`/`precious_metals`/`base_metals`/
  `agricultural`/`bulks`) ditambahkan ke seluruh 14 instrumen commodity —
  3 Layer 1 (`AU`/`AG`/`CL`) + 11 Layer 2 (termasuk 3 deferred:
  `TIN`/`CPO`/`RUBBER`).
- `instrument_loader.py`: dua field baru ditambahkan ke `Instrument`
  dataclass (default `None` — non-commodity instruments tidak terpengaruh).
  Dipopulasikan di kedua builder (`_build_commodity` untuk Layer 1,
  `_build_context_instrument` untuk Layer 2 via `_CONTEXT_CONSUMED_KEYS`).
- `sector_rotation.py`: `REGIME_SECTOR_WEIGHTS`'s flat `"commodity"` key
  diganti 5 key subcategory (`commodity_energy`, `commodity_precious_metals`,
  `commodity_base_metals`, `commodity_agricultural`, `commodity_bulks`) di
  seluruh 5 regime, nilai persis sesuai matrix Addendum §8.3. Lookup logic
  di `run()` diupdate: instrumen commodity sekarang route via
  `f"commodity_{inst.commodity_subcategory}"`, bukan `inst.market`.
- **Inkonsistensi internal ditemukan di Addendum sendiri, diperbaiki**:
  §7.1 men-declare enum value `precious_metals`, tapi §8.2's key-name
  table menyebut `commodity_precious` (tanpa `_metals`) — satu-satunya
  dari 5 key yang menyimpang dari formula mekanis
  `f"commodity_{subcategory}"` yang diikuti 4 key lainnya secara
  konsisten. Ditemukan empiris lewat test baru
  (`test_no_orphaned_commodity_subcategory`) yang gagal dengan
  `KeyError` — tanpa test ini, AU/AG akan diam-diam terdegradasi ke
  weight neutral 1.0 di setiap regime lewat `weights.get(key, 1.0)`nya
  alih-alih error. Diresolusi ke arah formula mekanis
  (`commodity_precious_metals`) — 4 dari 5 key lain sudah literal
  mengikutinya, jadi ini pilihan yang konsisten secara internal, bukan
  `commodity_precious`.
- `validate_instruments.py`: `commodity_role`/`commodity_subcategory`
  wajib untuk seluruh 14 instrumen commodity (termasuk yang deferred, per
  Addendum §7.1 "Required For: ALL commodity"), enum-validated. Fungsi
  `_validate_commodity_taxonomy()` dipakai bersama oleh Layer 1 dan Layer
  2 — satu validator, bukan dua yang bisa drift terpisah.
- **Test:** 16 test baru — `TestCommoditySubcategoryDisaggregation` (7,
  `test_sector_rotation.py`) + `TestCommodityTaxonomyValidation` (11,
  `test_validate_instruments.py`), termasuk
  `test_subcategory_to_weight_key_map_matches_sector_rotation_keys` — guard
  cross-module permanen yang secara spesifik akan menangkap ulang kelas
  bug `commodity_precious`/`commodity_precious_metals` di atas jika kedua
  modul kembali drift terpisah di masa depan.

EXPECTED_TOTAL tetap 699 (Layer 1=640, Layer 2=59), subcategories tetap
22 — ini adalah penambahan field-level taxonomy, bukan perubahan ukuran
universe.



Dokumen referensi: `GMI_Decision_Document_v1.docx` (ADR-013–019),
`GMI_Decision_Document_v2.docx` (ADR-020–025), `KNOWN_RISKS.md`
(RISK-7, RISK-8 baru; FP-AIO-001 baru)

Total: **13 ADR diimplementasikan** (7 dari Decision Doc v1, 6 dari
Decision Doc v2) + **1 bug independen ditemukan dan diperbaiki**
(FP-AIO-001) + **6 instance RISK-6 tambahan ditemukan dan diperbaiki**
(di luar 2 yang sudah diperbaiki v1.9.0) | **1188 passed / 0 failed / 0
error** (Δ +57 dari v1.9.0 — 1131)

Konteks: kedua Decision Document eksplisit menyatakan status **"DECIDED
— Nothing implemented"** pada saat penulisan. Rilis ini adalah
implementasi penuh dari kedua dokumen tersebut, dilakukan SEBELUM
melanjutkan ke GMI Wave 1 Cycle 4 (CrossAssetEngine) — persis prinsip
yang sama yang menjustifikasi audit v1.8.1 dan solidifikasi v1.9.0:
jangan bangun modul analitik canggih (CorrelationModule, LeadLagModule,
ForecastModule) di atas fondasi dependency yang rusak (CI tidak bisa
resolve pandas-ta sama sekali) atau config yang belum diverifikasi
(domain score weight-sum drift, instrument universe gap). Setiap
implementasi diverifikasi empiris sebelum diterapkan — termasuk
memverifikasi ulang **PyPI package state** (pandas-ta vs pandas-ta-classic,
langsung via `pypi.org/pypi/<pkg>/json`), **library output shape**
(pandas-ta-classic `ta.adx()`/`ta.bbands()` column names, terinstall dan
dipanggil langsung), dan **DuckDB glob semantics** (brace-alternation
TIDAK didukung — dibuktikan dengan percobaan langsung sebelum kode
produksi ditulis).

### ADR-020 [pyproject.toml, environment.yml, src/gold/indicators/pandas_indicators.py] — pandas-ta -> pandas-ta-classic migration

**Blocking dependency fix — tanpa ini, environment tidak bisa di-install
sama sekali.** Diverifikasi empiris terhadap PyPI: seluruh pandas-ta
0.3.x line (termasuk floor yang di-declare, 0.3.14) sudah dihapus dari
index; hanya tersisa dua prerelease build (0.4.67b0, 0.4.71b0), keduanya
`requires_python >=3.12`, dan bahkan di Python 3.12 constraint
`pandas-ta>=0.3.14` tetap gagal resolve (pip meng-exclude prerelease dari
floor eksplisit secara default). pandas-ta-classic adalah fork
community-maintained dari lineage 0.3.x yang sama, dengan rilis stabil
asli (hingga 0.6.52), `requires_python` kompatibel dengan floor project
(>=3.11 — **tidak dinaikkan ke 3.12**).

- `import pandas_ta as ta` -> `import pandas_ta_classic as ta` di kedua
  situs (`add_bbands()`, `add_adx()`).
- Diverifikasi langsung (bukan diasumsikan): `ta.adx()` pandas-ta-classic
  0.6.52 mengembalikan **`['ADX_14', 'DMP_14', 'DMN_14']`** — TIDAK ADA
  kolom ADXR. Artinya collision di balik FIX GLD-ADX-001 (v1.9.0) tidak
  bisa terjadi di fork ini. Guard `startswith("adx_")` **dipertahankan**
  (bukan di-revert ke `startswith("adx")`) — lebih presisi, gratis, dan
  tetap jadi guard hidup jika rilis mendatang memunculkan kolom serupa.
  `ta.bbands()` mengembalikan urutan kolom yang sama
  (`BBL_/BBM_/BBU_/BBB_/BBP_`) — wrapper existing tidak perlu berubah.
- **Test:** `test_pandas_indicators.py::test_adxr_is_not_silently_used_as_adx`
  diganti dengan `test_wrapper_adx_matches_raw_adx_column` (basic
  correctness) + `test_pandas_ta_classic_does_not_emit_adxr_column` (live
  regression guard, bukan asumsi tertulis di komentar). Kedua test
  fallback ImportError-simulation diupdate ke nama modul baru.

### ADR-021 [pyproject.toml, environment.yml] — scipy / statsmodels / hmmlearn promoted to hard dependencies

Sebelumnya hanya pip-installed ad hoc di sandbox tiap sesi, tidak pernah
di-declare di manifest — gap yang sudah di-flag sejak
`GMI_Implementation_Checkpoint.docx` dan dibawa terus tanpa resolusi
melalui v1.9.0. Wajib untuk GMI Wave 1 Cycle 4 (CorrelationModule
butuh scipy.linkage/fcluster + Ledoit-Wolf; LeadLagModule butuh
statsmodels Granger causality; ForecastModule butuh statsmodels VAR).
Di-declare SETELAH ADR-020 menetapkan floor Python, menghindari pola
kegagalan yang sama (pin dependency lalu baru temukan wheel gap).

### Companion actions — CI Python matrix + poetry.lock

- `.github/workflows/ci.yml`: `strategy.matrix.python-version: ['3.11',
  '3.12']` ditambahkan ke job `validate-and-test` — memverifikasi floor
  DAN ceiling benar-benar lolos CI, bukan sekadar diklaim.
- `poetry.lock` **baru** (113 packages, dihasilkan via `poetry lock`
  langsung, diverifikasi berisi `pandas-ta-classic==0.6.52`,
  `scipy==1.17.1`, `statsmodels==0.14.6`). `pyproject.toml`
  `[tool.poetry.dev-dependencies]` dimodernisasi ke
  `[tool.poetry.group.dev.dependencies]` (poetry 2.x deprecation
  warning yang muncul saat generate lock pertama kali).

### ADR-022 [scripts/check_glob_scope.py (NEW), .github/workflows/ci.yml] — CI Gate G-8: Layer 1/Layer 2 glob-scope enforcement

**Menutup dua pertanyaan audit terbuka `GMI_Implementation_Checkpoint_v3.docx`
§11.1/§11.3** sebagai gate permanen, bukan temuan manual yang harus
di-re-verify tiap kali: (a) double-`**` glob literal (RISK-2 defect
class), (b) glob `market_ohlcv` unfiltered yang tidak melalui
`silver_scope.py`'s `layer1_globs()`/`context_glob()` (RISK-6 defect
class). Scanner ditulis berbasis **AST** (bukan regex, tidak seperti
Gate G-2) — draft regex awal false-positive di docstring modul
`technical_signals.py` sendiri yang MEMBAHAS path lama sebagai sejarah,
bukan konstruksi live; AST membedakan docstring (`bare ast.Expr`
statement) dari string constant yang genuinely dipakai (assignment,
dict/list value, argumen call, fragment f-string).

**Saat gate ditambahkan, ditemukan 6 instance RISK-6 TAMBAHAN** di luar
2 yang sudah diperbaiki v1.9.0 (`quality_validator.py`,
`technical_signals.py`) — persis skenario yang diantisipasi ADR-022
sendiri. Semua 6 diperbaiki dalam rilis ini (bukan sekadar
di-grandfather), karena gate blocking yang gagal langsung saat merge
lebih buruk daripada tidak menambah gate sama sekali:

- **`src/backtest/pit_data.py`** [efisiensi, bukan korupsi data —
  query per-simbol dengan `WHERE symbol=$symbol` sudah menyaring benar]:
  `SILVER_OHLCV_PATH` (string tunggal unfiltered) -> `layer1_globs()`
  (list, dibind sebagai parameter DuckDB `$path`) di `get_ohlcv()` dan
  `get_ohlcv_universe()`. Modul ini sebelumnya nol test coverage
  dedicated (hanya tercakup insidental via `test_backtest_engine.py`,
  `test_fstring_sql_absence.py`, `test_preexisting_violations_v1.py`) —
  ketiganya di-rerun, 139 test lolos.
- **`src/gold/correlation_matrix.py`** [efisiensi — `active_symbols`
  filter sudah Layer-1-only]: `SILVER_1D_PATH` dihapus, `run()` sekarang
  membangun list via `layer1_globs()`, skip graceful jika kosong.
  **Modul ini sebelumnya NOL test coverage dedicated sama sekali** — file
  baru `tests/unit/test_correlation_matrix_glob_scope.py` (4 test)
  ditulis sebagai first-ever coverage, scoped khusus ke fix ini.
- **`src/gold/screener.py::_check_data_freshness`** **[P1 — bug korektnes
  nyata, bukan sekadar efisiensi]**: gate freshness ini persis pola
  masking-bug `quality_validator.py::_check_coverage` yang SUDAH
  diperbaiki v1.9.0 — `COUNT(DISTINCT symbol)` dari glob unfiltered
  dibagi denominator Layer-1-only (`get_loader().count()`). Beberapa
  anchor Layer 2 yang fresh bisa mendorong coverage% di atas angka
  Layer 1 sebenarnya, membuat gate yang SATU-SATUNYA tujuannya
  memblokir screener saat data Layer 1 stale bisa lolos padahal
  seharusnya blok. Fix: `layer1_globs()`. Dua test threshold existing
  (`test_screener_gld005.py`) yang hanya mock `duckdb.connect` (bukan
  filesystem) diupdate untuk juga mock `layer1_globs` — tanpa ini,
  guard baru "skip jika belum ada data Layer 1" membuatnya short-circuit
  sebelum mock DuckDB pernah dipanggil. + 1 test baru untuk graceful-skip.
- **`src/gold/views.py`** **[severity tertinggi — ini adalah Interface
  Contract (GD §0.4) untuk Trading Engine, consumer eksternal yang
  TIDAK bisa di-audit/dikoordinasikan pipeline ini]**: `v_ohlcv_1D` /
  `v_ohlcv_1H` / `v_ohlcv_all` sebelumnya bisa mengekspos VIX/DXY/ETF
  seolah-olah instrumen tradeable — persis yang SUDAH direklasifikasi
  keluar Layer 1 via ADR-003. **Percobaan pertama fix ini SALAH**: SQL
  list literal 4-market di-bake pada IMPORT TIME via `layer1_markets()`
  — langsung gagal karena `read_parquet()` dengan list argumen RAISE
  untuk SELURUH query jika SATU SAJA entry list-nya nol match (dibuktikan
  empiris; persis perilaku yang sudah didokumentasikan `silver_scope.py`
  untuk pola Python-list-parameter). Data Bronze/Silver datang bertahap
  per market saat runtime, bukan saat import — fixture test manapun yang
  hanya punya 1 dari 4 market akan gagal. Fix final: 3 SQL view
  ditulis sebagai TEMPLATE, di-resolve **saat call time** via
  `_resolve_ohlcv_view_sql()` yang memanggil `layer1_globs()` fresh
  setiap `get_pipeline_connection()` dipanggil (skip market yang belum
  ada data). DuckDB TIDAK mendukung brace-alternation glob (dibuktikan:
  `'{a,b}/**/*.parquet'` raise "No files found" walau kedua subdirektori
  ada) — SQL list literal adalah konstruk yang benar (dikonfirmasi
  bekerja standalone dan di dalam `CREATE VIEW`).
  **Percobaan kedua JUGA sempat trip Gate G-2** (f-string SQL) karena
  memakai f-string untuk interpolasi `view_name` + glob list — diperbaiki
  dengan concatenation + `_quoted_identifier()`, pola yang SUDAH
  established di file yang sama untuk alasan yang sama persis
  (identifier tidak bisa di-`$name`-bind di SQL engine manapun).
  3 test existing diperbaiki (pre-warm `get_loader()` sebelum
  `monkeypatch.chdir()`, karena `layer1_globs()` kini bergantung pada
  `InstrumentLoader` yang resolve `config/instruments.yaml` via path
  relatif) + 3 test BARU (`TestOhlcvViewsGlobScope`) membuktikan properti
  korektnes sebenarnya: VIX tidak bocor ke `v_ohlcv_1D`, view tetap
  bekerja walau baru 1 dari 4 market Layer 1 yang punya data (regression
  guard eksplisit untuk kegagalan percobaan pertama).
- **`src/utils/delta_reprocessor.py`**: `SILVER_GLOB` (default hardcoded
  unfiltered) -> sentinel `None`, `_effective_glob()` fallback ke
  `layer1_globs()` saat tidak di-override. Mekanisme override test
  existing (`monkeypatch.setattr(dr, "SILVER_GLOB", ...)`) dipertahankan
  utuh (6 test existing tetap lolos tanpa diubah) + 3 test baru untuk
  default path, termasuk bukti `find_stale_symbols()` tidak lagi
  melaporkan VIX (Layer 2) sebagai stale.
- **`src/utils/pipeline_dashboard.py`** [display-only, severity
  terendah]: baris "Silver OHLCV" dipecah jadi "Silver OHLCV (Layer 1)"
  dan "Silver OHLCV (Layer 2 context)" — perbaikan informasi genuine
  untuk operator, bukan sekadar gate-compliance. File baru
  `test_pipeline_dashboard_coverage.py` (2 test, first-ever coverage
  untuk modul ini).

**Test baru:** `tests/unit/test_check_glob_scope.py` (12 test untuk
scanner itu sendiri, termasuk regression guard eksplisit untuk false
positive docstring yang ditemukan saat menulis draft regex pertama, dan
`test_real_repo_currently_passes` yang menjalankan scanner terhadap repo
sungguhan sebagai live guard).

### ADR-013/ADR-014/ADR-015/ADR-016/ADR-024 [config/instruments.yaml, src/config/instrument_loader.py, src/utils/symbol_utils.py] — Layer 2 universe expansion: context_dollar_basket + context_fx_normalization

Melengkapi Broad Dollar Index Architecture v2.0 §7.2's basket 10-pasangan
— hanya 6 dari 10 pasangan yang di-desain (+ USD_IDR) yang sebelumnya ada
di universe manapun. 6 mata uang net-baru (CNH, KRW, SGD, HKD, TWD, NOK,
ADR-014) + 1 anchor point-fix untuk normalisasi CPO (MYR, ADR-024)
ditambahkan sebagai instrumen Layer 2 di bawah dua subcategory BARU:

- **`context_dollar_basket`** (ADR-014): 6 instrumen, `contributes_to: []`
  (nol bobot domain-score langsung — mencegah triple-counting DM/EM
  dollar strength lintas DXY + raw pairs + fitur derived Broad Dollar
  Index masa depan). CNH memakai offshore renminbi (ADR-013 — bukan
  onshore CNY, menghindari double-counting kebijakan PBOC yang sudah
  di-track via `context_rates_em_cb`). HKD disertakan dengan
  `reliability_flag: true` (ADR-015 — pegged currency, pola sama dengan
  SSEC/BOJ). SGD disertakan atas alasan FX-policy-band mandiri
  (ADR-016 — MAS mengelola kebijakan via S$NEER band, bukan interest
  rate).
- **`context_fx_normalization`** (ADR-024): 1 instrumen (MYR=X — bentuk
  kanonik Yahoo Finance, BERBEDA sengaja dari konvensi USD<CCY>=X yang
  dipakai 6 mata uang basket di atas), `contributes_to: []`,
  `include_in_forecast: false` (satu-satunya consumer: normalisasi CPO
  Silver-layer masa depan — bukan CrossAssetEngine/gold_signals/
  gold_screener). Sengaja TIDAK digabung ke `context_dollar_basket` —
  dua tujuan tak berhubungan (komputasi Broad Dollar Index vs. konversi
  mata uang satu komoditas) tetap terpisah.
- **ADR-017/ADR-018 [PARTIALLY GATED di dokumen sumber]**: nilai bobot
  basket persis dan magnitude override IDR TIDAK diimplementasikan di
  rilis ini — keduanya terblokir pada Gate 1 (ketersediaan data
  komponen bobot BIS EER), yang TIDAK bisa diverifikasi dari sandbox ini
  (tanpa akses network ke `stats.bis.org`). Prinsip ADR-018 (IDR
  di-override naik mengikuti presedan BI 2x-weighting) dicatat di
  komentar YAML untuk implementasi masa depan begitu Gate 1 selesai —
  BUKAN diimplementasikan sebagai angka konkret sekarang, konsisten
  dengan status "PARTIALLY GATED" dokumen sumber.
- `InstrumentLoader._CONTEXT_DIRECT_KEYS` diperluas dari `("dollar",)` ke
  `("dollar", "dollar_basket", "fx_normalization")` — mekanisme yang
  sama persis dipakai `dollar`/`context_dollar` yang sudah ada, tidak
  ada logic baru yang diperlukan.
- `scripts/validate_instruments.py`: `EXPECTED_TOTAL` 692 -> 699,
  `EXPECTED_SUBCATEGORIES` 20 -> 22, **BARU**
  `ZERO_WEIGHT_SUBCATEGORIES` guard (memastikan 2 subcategory baru tidak
  pernah diam-diam mendapat bobot domain-score).
- **Test:** 12 test baru di `TestGMIDecisionDocumentsV1V2`
  (`test_instrument_loader.py`) — mencakup setiap ADR individual
  (dollar_basket count, CNH offshore ticker, HKD reliability_flag, SGD
  documentation, MYR placement + forecast exclusion + ticker
  convention berbeda by design).

### ADR-019 [config/instruments.yaml] — Domain score weight-sum correction

**Audit penuh seluruh `_meta.contributes_to` block** (5 grup Layer 2:
dollar/rates/equity/commodity/etf) mereproduksi persis temuan
`GMI_Decision_Document_v1.docx` §3.4: 5 dari 8 domain score TIDAK sum ke
1.00 (`score_dollar_strength`=1.30, `score_yield_curve`=1.30,
`score_global_growth`=1.05, `score_inflation_pressure`=1.05,
`score_risk_appetite`=1.25) akibat kontributor undocumented tanpa
rasional yang bisa ditelusuri — sementara 3 lainnya (`score_commodity_cycle`,
`score_credit_stress`, `score_em_risk`) sudah persis cocok tabel dokumen
governing, bukti drift ini adalah kecelakaan yang tidak di-review, bukan
redesign yang disengaja.

Restorasi literal ke tabel Architecture Extension v1.0 §5.2 / Data
Source & Rates Adjustment v1.0 §7:
`context_rates_fed.contributes_to` dikosongkan total (0.30 + 0.20
undocumented dihapus); `context_rates_dm_cb` kehilangan entry
`score_yield_curve` (0.10, undocumented); `context_equity_dm` kehilangan
entry `score_risk_appetite` (0.25, undocumented); `context_etf_international`
kehilangan entry `score_global_growth` (0.05, undocumented);
`context_commodity_coal` kehilangan entry `score_inflation_pressure`
(0.22 — TERNYATA MENGGANTIKAN entry `context_rates_spread` yang
terdokumentasi di weight yang sama persis) — entry itu **dipulihkan** di
`context_rates_spread` sebagai gantinya; `context_etf_commodity`
kehilangan entry `score_inflation_pressure` (0.05, undocumented).

- **BARU** `scripts/validate_instruments.py::_validate_domain_score_weights()`
  — walk seluruh tree `context`, assert setiap score sum ke 1.00 exact
  (toleransi float 1e-9). Wajib per §9 Definition of Done dokumen sumber:
  *"All 8 domain scores' `_meta.contributes_to` weights sum to exactly
  1.00, verified by a new regression test."*
- **Test baru:** `TestDomainScoreWeightSumValidation` (3 test) —
  reproduksi langsung audit real-file, negative test yang menanam ulang
  SATU kontributor undocumented persis yang dihapus ADR-019
  (`context_rates_fed` -> `score_dollar_strength` 0.30) dan membuktikan
  validator menangkapnya, plus test enforcement `ZERO_WEIGHT_SUBCATEGORIES`.

### ADR-023 [config/instruments.yaml] — Commodity MYR-normalization scope correction

**Tiga dokumen governing berbeda pendapat** — diverifikasi via
Architecture v2.1 Addendum §4.2/§5.3 sebagai sumber paling spesifik dan
terbaru: dari 3 komoditas Wave 2 yang deferred (TIN, CPO, RUBBER), HANYA
CPO yang genuinely MYR-dependent. TIN (LME, tvdatafeed `SN`) dan RUBBER
(SICOM/SGX, tvdatafeed `SICOM_TSR20`) adalah USD-native — Addendum §5.3
secara eksplisit menolak TOCOM RSS3 (JPY) untuk RUBBER demi menghindari
risiko timestamp-conversion/lookahead, keputusan ADR yang disengaja,
bukan oversight.

- `TIN`/`RUBBER`: `deferred_reason` diperbaiki dari klaim normalisasi MYR
  ke verifikasi ticker/exchange tvdatafeed (OD-C1, tidak terkait mata
  uang); `requires_fx_normalization: false`; `base_currency: USD`.
  Status `context_available: false` **TIDAK berubah** — hanya alasan
  blocking yang dikoreksi.
- `CPO`: tidak berubah (`requires_fx_normalization: true`,
  `base_currency: MYR`) — kini didokumentasikan sebagai satu-satunya
  instrumen MYR-dependent, akan dinormalisasi via anchor MYR=X baru
  (ADR-024) begitu Wave 2 diimplementasikan.
- **Test:** `test_adr023_only_cpo_is_myr_dependent` (di
  `TestGMIDecisionDocumentsV1V2`) mengunci ketiga instrumen per-simbol,
  bukan asersi uniform seperti sebelumnya. `test_deferred_instruments_have_required_fields`
  (`test_instrument_loader.py`) ditulis ulang total — assert uniform
  lama (`requires_fx_normalization is True` untuk SEMUA deferred) sudah
  tidak valid post-ADR-023, diganti per-simbol.

### Companion action — high_52w/low_52w rename (finnhub_ingester.py)

Nama field misnomer: field `h`/`l` Finnhub adalah high/low HARI
TRADING SAAT INI, bukan rentang 52-minggu. Direname ke `day_high`/
`day_low` di `finnhub_ingester.py`, `config/schemas/finnhub_quote.yaml`.

**Premis dokumen sumber ("zero current consumers, zero migration cost")
TERBUKTI SALAH saat verifikasi empiris** — `src/silver/fundamental_processor.py
::process_quotes()` adalah consumer nyata dan live, membaca kolom ini
langsung dari Bronze via SQL SELECT dan menulisnya utuh ke Silver. Fix
memperbarui SQL SELECT di `process_quotes()` juga (bukan hanya sisi
Bronze) — konsisten dengan prinsip empirical-verification-over-
documentation-trust project ini.

### FIX FP-AIO-001 [P1, ditemukan tidak direncanakan] — atomic_write_parquet tidak pernah di-import di fundamental_processor.py

**Ditemukan sebagai efek samping** menulis test end-to-end pertama untuk
`process_quotes()` (bagian rename di atas) — bukan item yang direncanakan
dari ADR manapun. `atomic_write_parquet()` dipanggil di DUA situs
(`process_earnings()`, `process_quotes()`) tapi **tidak pernah
di-import** di file ini — `NameError` pada setiap invocation NYATA
(non-empty). Test graceful-no-data existing untuk kedua method
(`test_process_earnings_graceful_no_bronze`,
`test_process_quotes_graceful_no_bronze`) return awal sebelum mencapai
baris write, persis mengapa ini tidak pernah terdeteksi — kelas akar
masalah yang sama dengan FIX GLD-ADX-001 (v1.9.0): jalur invocation
nyata dengan nol test coverage sampai baru ditulis di sesi ini.

- `from src.utils.atomic_io import atomic_write_parquet` ditambahkan.
- **Test baru:** `test_process_quotes_reads_day_high_day_low` dan
  `test_process_earnings_writes_real_data` — first-ever invocation nyata
  (non-empty data) untuk KEDUA method, bukan hanya method yang memicu
  penemuan bug.

### ADR-025 [scripts/preflight/*.py (NEW)] — External-source verification: authored now, executed when network-enabled

Tiga script pre-flight ditulis sebagai artifact checked-in, TIDAK
dieksekusi terhadap API live (sandbox sesi ini juga tidak punya akses
network ke yfinance/BIS/Finnhub — constraint yang sama persis
didokumentasikan di setiap checkpoint GMI sebelumnya):

- `check_yfinance_tickers.py` — verifikasi shape OHLCV untuk semua
  instrumen Layer 2 aktif, perhatian khusus ke 7 mata uang baru rilis
  ini (Gate 2 kedua dokumen keputusan menandai KRW/SGD/HKD/TWD/NOK
  sebagai live-unconfirmed; CNH dan MYR masing-masing dikonfirmasi via
  ADR-013/ADR-024).
- `check_bis_cbpol_d.py` — verifikasi resolusi harian (bukan bulanan)
  utuh 12 REF_AREA code, menutup Data Source & Rates Adjustment v1.0
  §13 checklist item □1/□2 yang terus dibawa "belum diverifikasi" sejak
  checkpoint pertama.
- `check_finnhub_shape.py` — verifikasi shape response live `/quote` dan
  `/calendar/earnings` terhadap kontrak yang di-declare schema (dibangun
  via web search di v1.9.0, bukan panggilan API live).
- **Test:** `test_preflight_scripts.py` (14 test) — mencakup logic murni/
  network-independent SAJA (date-math resolusi harian, shape-check
  terhadap client yang di-mock, penanganan prerequisite hilang) —
  sengaja TIDAK mencoba test panggilan network nyata, konsisten dengan
  framing ADR-025 sendiri.

### Test Suite Summary

| Checkpoint | Total | Δ |
|---|---|---|
| Inherited baseline (v1.9.0) | 1131 | — |
| + ADR-020 (pandas_indicators rename) | 1132 | +1 |
| + TestGMIDecisionDocumentsV1V2 + TestDomainScoreWeightSumValidation | 1147 | +15 |
| + high_52w/low_52w rename + FP-AIO-001 | 1150 | +3 |
| + Gate G-8 (scanner + 6 file fixes + their tests) | 1174 | +24 |
| + ADR-025 preflight script tests | 1188 | +14 |

**Reconciled: 1131 + 57 = 1188.** Cocok persis dengan hasil test suite
akhir yang diverifikasi ulang.



Dokumen referensi: `KNOWN_RISKS.md` (RISK-4 resolved; RISK-5, RISK-6 baru)

Total: **6 finding diperbaiki** (1 refactor arsitektural + 3 bug fix + 1
critical pre-existing bug + 1 dormant risk resolved) | **22 file**
(9 baru, 13 dimodifikasi) | **1131 passed / 0 failed / 0 error**
(Δ +76 dari v1.8.1 — 1055)

Konteks: eksplisit permintaan user untuk **menyelesaikan dan
mensolidkan modul crucial/critical di Bronze-Silver-Gold SEBELUM**
melanjutkan ke GMI Wave 1 Cycle 4 (CrossAssetEngine) — persis prinsip
yang sama yang menjustifikasi audit v1.8.1: jangan bangun statistik yang
canggih (Ledoit-Wolf correlation, Granger-causality lead-lag, HMM regime)
di atas fondasi yang belum benar-benar diverifikasi. Setiap temuan di
bawah ini ditemukan secara **empiris** (baca kode aktual, jalankan probe
nyata terhadap DuckDB/Polars/pandas-ta yang benar-benar terinstall) —
bukan diasumsikan dari dokumentasi desain.

### ADD GMI-CTX-001 — Separation of Concerns: context_anchors.py dipisah dari active_symbols.py

Permintaan eksplisit user: `ActiveSymbolsResolver` melakukan dua pekerjaan
yang secara struktural tidak berhubungan — Layer 1 (query DuckDB
liquidity-screened yang sudah diaudit, AS-1..AS-12, dilindungi
KNOWN_RISKS.md sebagai "jangan disentuh tanpa correctness defect
konkret") dan Layer 2 (passthrough config-driven, tanpa Silver query sama
sekali — Architecture v2.0 §4.2: *"Filter: None"*). Blast-radius check
sebelum ekstraksi (bukan asumsi): `resolve_context()`/`load_context()`/
`load_context_full()` nol caller di luar `active_symbols.py` dan file
test-nya sendiri — memungkinkan **clean break**, bukan migrasi
compatibility-shim.

- **NEW** `src/silver/context_anchors.py::ContextAnchorsResolver` —
  `resolve()`/`load()`/`load_full()` (drop qualifier "_context" — tidak
  ambigu lagi di kelas sendiri). Output path BARU:
  `data/silver/context_anchors/context_anchors_{date}.parquet` (bukan
  legacy-alongside-new seperti `active_ohlcv_{date}.parquet`, karena
  blast-radius check membuktikan tidak ada consumer path lama).
- **NEW job** `silver_context_anchors` — `depends_on: []` **sengaja**,
  bukan shortcut: `resolve()` murni enumerasi InstrumentLoader, nol
  Silver read, jadi fake dependency ke `silver_ohlcv` hanya menambah
  risiko blocking tanpa alasan nyata (lihat docstring `run()` untuk
  rationale lengkap).
- `active_symbols.py` sekarang Layer 1 murni — `resolve()`/`_RESOLVE_QUERY`/
  `_SCREENED_LIMIT=175` **tidak disentuh sama sekali** (diverifikasi:
  hanya baris `resolve_context()`/`load_context()`/`load_context_full()`
  dan satu call site di `run()` yang dihapus).
- **Test:** `tests/unit/test_context_anchors.py` (13 test — migrasi 4 dari
  `test_active_symbols.py` + 9 baru, termasuk `load()`/`load_full()` yang
  **sebelumnya nol test coverage sama sekali**, di bawah nama apapun).
  `tests/integration/test_job_registry_integrity.py::TestGMIJR003ContextAnchorsWiring`
  (10 test) termasuk negative test eksplisit
  (`test_active_symbols_resolver_no_longer_exposes_layer2_methods`) yang
  membuktikan ekstraksi benar-benar terjadi, bukan sekadar modul baru
  ditambah di samping yang lama.

### FIX QV-L2-01 / GLD-L2-01 [P1] — Layer 1 checks silently scanned Layer 2 rows

**RISK-6 di KNOWN_RISKS.md.** Root cause tunggal, tiga symptom: setiap
consumer `data/silver/market_ohlcv/` (5 check `quality_validator.py`,
Silver read `technical_signals.py`) memakai glob rekursif tanpa filter
market. Sejak GMI Cycle 3 menambah Layer 2 context OHLCV di root yang
SAMA (`market_ohlcv/context/...`), glob tanpa filter itu **diam-diam
ikut scan baris Layer 2**. Dibuktikan empiris (bukan diasumsikan) via
probe DuckDB langsung sebelum fix ditulis:

- `_check_coverage`: simbol Layer 2 menggembungkan numerator
  `COUNT(DISTINCT symbol)` terhadap denominator Layer-1-only
  (`get_loader().count()`) — coverage% bisa terbaca >100% dari angka
  Layer 1 sebenarnya, menyembunyikan penurunan coverage Layer 1 yang
  nyata di bawah gate 95%.
- `_check_freshness`: satu anchor Layer 2 yang fresh (mis. VIX)
  menyembunyikan staleness Layer 1 pipeline-wide, karena `MAX(timestamp)`
  dihitung lintas kedua layer sekaligus. **Reproduksi empiris**: AAPL
  (Layer 1) 19 hari stale + VIX (Layer 2) fresh hari ini → pre-fix,
  freshness_check melaporkan lag 0 hari (PASS yang salah).
- `technical_signals.py::_process_timeframe`: RSI/MACD/ADX/BBands
  dihitung untuk VIX, DXY, 13 global index, 25 ETF, 8 commodity context
  seolah-olah tradeable candidates — bertentangan langsung dengan
  rationale ADR-003 sendiri untuk reklasifikasi VIX/DXY keluar Layer 1
  ("RSI pada VIX... tidak bermakna").

**Fix:** utility baru `src/utils/silver_scope.py`
(`layer1_globs()`/`context_glob()`) — market list Layer 1 diturunkan dari
`InstrumentLoader`, bukan hardcode; skip market directory yang belum ada
(bukan include glob mati yang bikin DuckDB `read_parquet($list)` raise
untuk SELURUH list ketika satu entry nol-match — diverifikasi empiris).
Dipakai di `quality_validator.py` (5 check di-scope ulang) dan
`technical_signals.py` (Silver read di-scope ulang). **Layer 2 check
suite baru** ditambahkan sekaligus — `_check_context_null/_price_sanity/
_coverage/_gap_detection/_outlier_detection/_freshness` — level
**WARNING**, sengaja bukan CRITICAL: belum ada Gold-layer consumer Layer
2 Silver OHLCV (CrossAssetEngine = Cycle 4, belum dibangun); mem-block
seluruh Gold layer karena satu anchor Layer 2 bermasalah hari ini
melanggar Separation of Concerns (over-coupling consumer yang belum ada).

### ADD GLD-ACTIVE-001 [Architecture v2.0 §5.2] — gold_signals active_ohlcv filter

Spec Architecture v2.0 (~190 Layer 1 candidates, bukan seluruh 640) tidak
pernah diimplementasikan. `_resolve_active_ohlcv_symbols()` baru —
`ActiveSymbolsResolver.load_ohlcv(run_date)`, fallback graceful ke Layer
1 penuh (degraded tapi benar, bukan crash) jika `silver_active_symbols`
belum jalan untuk `run_date` — hanya relevan untuk invocation langsung/
out-of-sequence; `DependencyGuard` produksi sudah menjamin urutan yang
benar. Filter diterapkan via DuckDB `= ANY($active_symbols)` (list-bound
parameter, diverifikasi bekerja — bukan f-string SQL).

### FIX GLD-ADX-001 [P0, CRITICAL] — add_adx() crash pada setiap invocation nyata

**RISK-5 di KNOWN_RISKS.md. Ditemukan tidak sengaja** saat menulis test
pertama untuk `technical_signals.py`/`pandas_indicators.py` — **keduanya
nol test coverage sebelumnya**, dikonfirmasi via grep penuh `tests/`.
`add_adx()` me-rename kolom output `ta.adx()` via
`lc.startswith("adx")` — pandas-ta versi terinstall (0.4.71b0)
mengembalikan EMPAT kolom, bukan tiga: `['ADX_14', 'ADXR_14_2', 'DMP_14',
'DMN_14']`. `ADXR` (Average Directional Index **Rating**, varian smoothed
yang tidak dipakai pipeline ini) JUGA match `startswith("adx")` — kedua
kolom di-rename ke nama target yang SAMA (`"adx"`), menghasilkan
DataFrame pandas dengan nama kolom duplikat.
`pl.from_pandas()` **benar** menolak ini (`ValueError: non-unique
indices and/or column names`) — artinya **setiap** invocation nyata
`add_adx()` (= setiap invocation nyata `gold_signals`, dan semua yang
bergantung padanya: `gold_mtf`, `gold_screener`) **raise**. Dikonfirmasi
bug ini **byte-identical dengan pristine v1.8.1** (diff langsung) — pre-
existing, bukan regresi dari sesi ini.

**Fix:** match `lc.startswith("adx_")` (dengan underscore) — tetap match
`ADX_14`, benar-benar exclude `ADXR_14_2` (`"adxr_14_2".startswith("adx_")`
= `False`, karakter setelah "adx" adalah "r" bukan "_"). `add_bbands()`
diaudit dengan output pandas-ta nyata yang sama, dikonfirmasi TIDAK
punya collision serupa (`BBB_`/`BBP_` tidak match pattern upper/mid/lower
manapun) — tidak ada perubahan diperlukan di sana.

**Test:** `tests/unit/test_pandas_indicators.py` (10 test, file pertama
untuk modul ini) — termasuk `test_adxr_is_not_silently_used_as_adx` yang
menghitung ADX/ADXR independent lalu assert kolom wrapper `adx` cocok
dengan ADX (bukan hanya "tidak crash"), sehingga collision serupa di masa
depan (pandas-ta versi baru menambah kolom ADX-prefixed lain) tertangkap
sebagai assertion failure, bukan silent value-swap.

### FIX RISK-4 [KNOWN_RISKS.md] — finnhub_ingester.py: zero schema validation → RESOLVED

Blocker asli (butuh schema nyata, sandbox tidak ada akses network ke
Finnhub) diselesaikan via **web search terhadap dokumentasi resmi
Finnhub** (`/quote`, `/calendar/earnings` response shape — field names,
types, nullability) — blocker itu untuk *response data*, bukan
*dokumentasi*, dan schema hanya butuh yang terakhir.
`config/schemas/finnhub_quote.yaml` + `finnhub_earnings_calendar.yaml`
baru. Kedua write path (`_ingest_earnings_calendar`, `_ingest_symbol`)
sekarang gate lewat `SchemaValidator` sebelum `write()`.

**Fragility kedua ditemukan sekaligus** (bukan trivial mechanical wiring):
ingester ini fetch window 90-hari FORWARD, jadi `eps_actual` genuinely
`None` untuk hampir semua baris di operasi nyata — kasus NORMAL, bukan
anomali. Membiarkan Polars infer dtype dari raw dict value berarti kolom
all-`None` infer `Null` dtype, bukan `Float64` — gagal validasi pada
kasus paling umum. `revenueEstimate` juga bisa datang sebagai integer
JSON polos (tanpa titik desimal) — batch all-integer infer `Int64`
terhadap schema `Float64`. Kedua write path sekarang eksplisit
`.cast(pl.Float64, strict=False)`/`.cast(pl.Int64, strict=False)` setiap
kolom SEBELUM validasi — kontrak schema stabil terlepas dari value apa
yang kebetulan ada di satu fetch.

**Tidak disentuh:** `_bronze_finnhub`'s `NotImplementedError` block
(job_registry.py) — fix ini mengeraskan `FinnhubIngester` untuk saat blok
itu dicabut nanti ("Finnhub Integration" roadmap), tidak mencabutnya.
Misnomer `high_52w`/`low_52w` (sebenarnya high/low HARI INI, bukan
52-week) didokumentasikan di komentar YAML, tidak di-rename — di luar
scope fix validasi schema, dan nol consumer membaca data ini saat ini.

**Test:** `tests/unit/test_finnhub_ingester.py` (16 test, file pertama
untuk modul ini) — termasuk regression guard langsung untuk kedua
fragility di atas (`test_all_null_eps_actual_does_not_cause_spurious_quarantine`,
`test_integer_shaped_revenue_estimate_does_not_cause_spurious_quarantine`).

### Diverifikasi

`ast.parse()` OK pada seluruh file .py yang dimodifikasi/dibuat (9 baru +
13 dimodifikasi); full suite **1131 passed / 0 failed / 0 error** (dari
1055 → +76); `validate_instruments.py` exit 0 (692 symbols, tidak
terpengaruh); setiap fix di atas punya regression test yang secara
spesifik mereproduksi bug asli sebelum membuktikan fix-nya, bukan hanya
"tidak crash lagi".

---



Dokumen referensi: `KNOWN_RISKS.md` (RISK-2, RISK-3, RISK-4)

Total: **1 audit pass** | **6 file dimodifikasi** (2 fix, 3 test, 1 doc) |
**1055 passed / 0 failed / 0 error** (Δ +19 dari v1.8.0 — 1036)

Konteks: Cycle 3 (v1.8.0) menutup foundational gap Layer 2 OHLCV, tapi
dalam prosesnya menemukan bug yang sudah silently live sejak sebelum
sesi ini dimulai (glob DuckDB double-`**`, GMI-GLD-001) — disembunyikan
sepenuhnya oleh `except Exception: pass`. Sinyal itu cukup kuat untuk
menjustifikasi audit formal Bronze+Silver SEBELUM lanjut ke Cycle 4
(CrossAssetEngine) — membangun HMM regime detection, Ledoit-Wolf
correlation, dan Granger-causality lead-lag di atas fondasi yang belum
diverifikasi selain "compile dan lolos test sintetis" adalah urutan
pekerjaan yang salah untuk platform yang seluruh nilainya bergantung pada
sinyal yang bisa dipercaya.

### Audit dilakukan (mekanis, terverifikasi empiris — bukan grep sekali lalu percaya)

- **RISK-2 (CLOSED — audited, tidak fixed lagi karena tidak ada yang perlu difix):**
  Scan komprehensif seluruh `src/` tree (bukan hanya Bronze/Silver) untuk
  pola double-`**` dalam satu glob string. Ditemukan hanya 2 instance lain
  di luar yang sudah difix di v1.8.0 — keduanya di `ohlcv_processor.py`
  (PASS 1 loop Layer 1 yang sudah ada, dan `run_context()` Layer 2 baru),
  dan keduanya memakai `pl.scan_parquet()` (Polars) — diverifikasi
  empiris Polars TOLERAN terhadap pattern yang DuckDB tolak. Kesimpulan:
  bug class ini adalah **insiden terisolasi**, bukan pola sistemik.
- **RISK-3 (FIXED):** Investigasi mendalam terhadap `test_fstring_sql_absence.py`
  (test yang sudah ada dari audit GLD-003 sebelumnya) mengungkap root
  cause ganda: (1) scope GLD-003 secara eksplisit TIDAK mencakup
  `sector_rotation.py`/`views.py`, dan (2) scanner function-nya sendiri
  hanya mendeteksi f-string TRIPLE-quote — kedua violation memakai
  SINGLE-quote f-string yang bahkan tidak akan terdeteksi sekalipun
  file-nya ada di scope. **Kedua akar masalah diperbaiki sekaligus**, bukan
  hanya gejalanya.
- **Layer Independence Guarantee (GD §17.2):** Verifikasi mekanis lintas
  seluruh `src/bronze/`, `src/silver/`, `src/gold/` — Bronze tidak pernah
  baca Silver/Gold, Silver tidak pernah baca Gold, Gold tidak pernah baca
  Bronze langsung, Silver tidak pernah panggil external API kecuali
  `sentiment_processor.py` (exception yang memang didesain GD §17.4).
  **Hasil: BERSIH, nol pelanggaran.**
- **Schema validation coverage:** Semua 10 Bronze ingester dicek satu per
  satu. 8 dari 10 punya `SchemaValidator` yang aktif. `treasury_ingester.py`
  legitimate (delegasi ke `FREDIngester` yang sudah punya validator sendiri).
  `finnhub_ingester.py` — **RISK-4 baru, lihat di bawah.**
- **Atomic write compliance:** 4 call site (`active_symbols.py` ×3,
  `global_rates_processor.py` ×1) memakai `write_parquet()` langsung,
  BUKAN via `atomic_write_parquet()` shared utility — diverifikasi tiap
  satu: semuanya reimplementasi manual pattern `tempfile` + `os.replace`
  yang IDENTIK dan tetap atomic secara benar. Bukan correctness bug,
  dicatat sebagai catatan konsolidasi minor di `KNOWN_RISKS.md`.

#### FIX GMI-AUD-001 [P2] — src/gold/sector_rotation.py

`_get_active_regime()`: f-string SQL (`f"SELECT regime FROM
read_parquet('{regime_path}')" f" WHERE date = '{run_date}' LIMIT 1"`) →
`$path`/`$run_date` parameterized binding. `str(run_date)` dipakai (bukan
raw `date` object) untuk mempertahankan semantik perbandingan STRING yang
persis sama dengan versi f-string asli.

#### FIX GMI-AUD-002 [P2] — src/gold/views.py

`register_views()`/`list_available_views()`: f-string SQL identifier
interpolation (`f"SELECT COUNT(*) FROM {view_name} LIMIT 1"`) TIDAK bisa
diperbaiki dengan `$name` binding — parameter binding di SQL engine
manapun hanya untuk VALUE, tidak pernah untuk identifier di posisi FROM.
Fix: `_quoted_identifier()` helper baru — validasi regex
`^[A-Za-z_][A-Za-z0-9_]*$`, quote dengan tanda kutip ganda, raise
`ValueError` untuk apapun selain itu — dipakai via string concatenation
biasa (bukan f-string). `view_name` selalu berasal dari
`VIEW_DEFINITIONS.keys()` (dict hardcoded internal) — risiko injection
nol secara struktural, tapi guard tetap ditambahkan sebagai defense-in-depth.

#### ADD GMI-AUD-003 — tests/unit/test_fstring_sql_absence.py

Scanner AST-based baru (`_scan_fstring_sql_violations_ast`) menggantikan
pendekatan character-window lama sebagai regression guard PRIMER: presisi
(hanya melihat konten literal node `ast.JoinedStr` itu sendiri, bukan
"dalam radius 400 karakter dari SQL asli di fungsi yang sama" yang
menghasilkan 7 false positive terverifikasi selama audit ini), dan
berjalan di SELURUH `src/` tree secara permanen — menutup pola "diaudit
sebagian, file demi file, dengan scope tiap audit lebih sempit dari
codebase" yang menjadi root cause RISK-3 tidak pernah ketemu sebelumnya.

**Ditemukan tapi TIDAK diperbaiki di pass ini** (RISK-4 baru,
`KNOWN_RISKS.md`): `finnhub_ingester.py` menulis ke Bronze tanpa schema
validation sama sekali — GD §3.7 secara eksplisit menyebut Finnhub
sebagai motivating example untuk kenapa schema validation dibutuhkan.
Blast radius saat ini DORMANT (`_bronze_finnhub` wrapper di
`job_registry.py` sengaja selalu `NotImplementedError` — belum reachable
via pipeline), tapi perlu diperbaiki SEBELUM roadmap item "Finnhub
Integration" dimulai. Tidak diperbaiki sekarang karena butuh desain
schema nyata (field list, types, nullability) yang idealnya diverifikasi
terhadap live API response — sandbox ini tidak punya akses network ke
Finnhub untuk itu; mendesain schema dari field list kode saja berisiko
mengkodekan asumsi sebagai fakta.

**Diverifikasi:** `ast.parse()` OK pada 133 file .py; 1055 passed / 0
failed / 0 error (dari 1036 → +19); CI Gate G-2 bersih (`grep -rn
'f"SELECT...'` → nol hasil); `validate_instruments.py` exit 0 (692 symbols,
tidak terpengaruh audit ini).

---

## v1.8.0 — GMI Wave 1: Dual-Layer Universe + BIS Rates + Layer 2 OHLCV Pipeline (Juli 2026)

Dokumen referensi: `GMI_Implementation_Checkpoint.docx` (handoff dari sesi sebelumnya)
                   `alpha_factory_architecture_v2.docx`
                   `alpha_factory_architecture_extension_v1.docx`
                   `alpha_factory_data_source_rates_adjustment_v1_0.docx`

Total: **3 cycles (Cycle 1-3) dikonsolidasikan** | **13 file dimodifikasi** |
**1036 passed / 0 failed / 0 error** (Δ +35 tests dari baseline v1.7.7 — 1001)

Catatan versi: Cycle 1 dan 2 diimplementasikan dan diuji lengkap di sesi
sebelumnya (checkpoint doc) tapi **tidak pernah dipaketkan sebagai zip
berversi** — working copy hanya ada di sandbox ephemeral sesi tersebut,
tanpa entry CHANGELOG, tanpa version bump. v1.8.0 ini adalah rilis
konsolidasi pertama untuk seluruh GMI Wave 1 (Cycle 1-3 sekaligus) — bukan
tiga rilis terpisah, karena tidak pernah ada build yang benar-benar
"shipped" tanpa Cycle 3 (universe 692-instrumen yang dideklarasikan tanpa
Cycle 3 tidak punya data OHLCV Layer 2 sama sekali — state yang secara
sengaja tidak dijadikan checkpoint version boundary).

---

### GMI Wave 1 Cycle 1 — Dual-Layer Universe Foundation (692 instruments)

**instruments.yaml v1.2 → v1.4, InstrumentLoader dual-layer rewrite, 20 Layer 2 subcategories**

- Universe diperluas 643 → 656 (Architecture v2.0: +13 global equity
  indices) → 692 (Architecture Extension v1.0: +36 — 11 commodity context,
  25 ETF context) → tidak berubah lagi di Data Source & Rates Adjustment
  v1.0 (12 CB rates = macro series, bukan OHLCV instrument, tidak
  menambah EXPECTED_TOTAL).
- SPX, VIX, DXY direklasifikasi dari Layer 1 (us_stocks.Index / forex) ke
  Layer 2 context (ADR-003) — Layer 1 turun dari 643 ke 640.
- Layer 2 final: 52 OHLCV instruments (49 aktif + 3 deferred Wave 2:
  TIN/CPO/RUBBER — MYR→USD normalization pipeline belum ada, ADR-007),
  dalam 20 subcategories lintas 5 groups (dollar/rates/equity/
  commodity/etf).
- `InstrumentLoader` ditulis ulang: Layer 1 API (`all_symbols()`,
  `get()`, `by_market()`, `count()`) **100% backward-compatible**, ditambah
  API Layer 2 baru: `all_context()`, `by_context_category()`,
  `by_context_group()`, `forecast_context()`, `correlation_context()`,
  `deferred_count()`, `get_context()`, `subcategory_meta()`,
  `all_subcategory_ids()`.
- Diverifikasi empiris (bukan asumsi) sebelum konsolidasi ini:
  `loader.all_context(include_deferred=False)` → 49 instruments, breakdown
  `{etf:25, equity:15, commodity:8, dollar:1}`; `loader.count()` → 640;
  `loader.count_total()` → 689; `validate_instruments.py` →
  `VALIDATION PASSED — 692 symbols (Layer 1=640, Layer 2=52)`.
- Tag: `# ADD GMI-IL-001`, `# ADD GMI-VAL-001`, `# ADD GMI-AS-001`

---

### GMI Wave 1 Cycle 2 — BIS Central Bank Rates Infrastructure (13 CBs)

**bis_rates_ingester.py, global_rates_processor.py, bis_cb_rates.yaml**

- ECB dikoreksi dari FRED (ECBDFR, monthly — cadence mismatch untuk daily
  computation) ke BIS Statistics Warehouse CBPOL_D (daily, ADR-010).
- 12 CB non-FED (ECB, BOE, BOJ, BOC, RBA, RBNZ, SNB, NORGES, RIKSBANK,
  PBOC, BOK, BI) via satu ingester tunggal `bronze_bis_rates` — no API
  key, CSV format, weekly cadence (WEEKLY_SEQUENCE).
- `silver_global_rates` — tabel PIT terpisah dari `silver_macro_enriched`
  (semantik `effective_date` vs `observation_date` berbeda fundamental,
  §9.1) — forward-fill, structural break flags (BI_RATE 2016-08-19,
  PBOC_RATE COVID 2020, BOJ_YCC 2016-2024).
- `job_registry.py`: `silver_active_symbols` wrapper diperbaiki untuk
  delegate ke module-level `run()` (yang resolve Layer 1 DAN Layer 2),
  bukan memanggil `resolver.resolve()` langsung (yang silently skip
  Layer 2) — FIX GMI-JR-001.
- Tag: `# ADD GMI-JR-001`

---

### GMI Wave 1 Cycle 3 — Layer 2 Context OHLCV Pipeline [BLOCKING, closes foundational gap]

**Bronze/Silver ingestion untuk 49 Layer 2 context anchors aktif — sebelum ini nol.**

#### GMI-BRZ-001 [BLOCKING] — src/bronze/market_ingester.py

**Root cause:** `MarketOHLCVIngester.run()` hanya meng-iterate
`loader.all_symbols()` (Layer 1, 640 trading candidates). 49 Layer 2
context anchors aktif (VIX, DXY, 13 global equity indices, 25 ETF, 8
commodity context) tidak pernah punya OHLCV data di Bronze sama sekali —
setiap consumer Gold-layer Layer 2 yang direncanakan (CrossAssetEngine,
GlobalIndexRegimeModule, gold_domain_scores — Architecture v2.0 §6,
Architecture Extension v1.0 §5) tidak akan punya raw price data untuk
beroperasi begitu diimplementasikan.

**Diverifikasi empiris sebelum implementasi (bukan asumsi):**
- `to_api_symbol('DXY', 'context', 'yfinance')` mengembalikan `'DXY'`
  (SALAH — seharusnya `'DX-Y.NYB'`). Tidak ada cabang untuk
  `market='context'` di `symbol_utils.py`; fallback `YFINANCE_SUFFIX.get(market, "")`
  mengembalikan raw symbol tanpa transformasi. `instruments.yaml` v1.4
  sudah menyimpan `yfinance_symbol` siap-pakai per instrumen Layer 2 —
  dipakai LANGSUNG, `to_api_symbol()` di-bypass sepenuhnya untuk Layer 2.
- `YFinanceAdapter.fetch()` sudah market-agnostic (docstring: "api_symbol
  must already be in yfinance format") — reuse 100% tanpa modifikasi.
- `_fetch()`'s market dispatch sudah punya cabang `else` (yfinance-only,
  no fallback chain) yang otomatis berlaku untuk `market='context'` —
  reuse tanpa modifikasi.
- `IncFetchProtocol.resolve_start_date()`, `BronzeIngester.write()`,
  `SchemaValidator` (`config/schemas/yfinance_ohlcv.yaml` — kolom lowercase,
  `volume` nullable "some instruments have null volume (indices)", sudah
  cocok untuk VIX/global index tanpa perubahan) — semuanya generic
  terhadap `market` sebagai path segment string; nol modifikasi.

**Opsi dipertimbangkan** (Bronze write-path convention untuk Layer 2):
(A) partisi per `context_group` (`market/ohlcv/context/{group}/...`) —
DITOLAK: butuh modifikasi `IncFetchProtocol`/glob patterns untuk
mendukung depth tambahan, manfaat (browsability per grup) tidak
dibutuhkan consumer manapun saat ini. (B — **dipilih**) satu bucket
`context` flat (`market/ohlcv/context/...`), identik dengan pola setiap
`inst.market` Layer 1 lainnya — nol modifikasi ke infrastruktur shared;
`context_group`/`context_category` tetap queryable via
`InstrumentLoader.get_context(symbol)` (single source of truth) tanpa
didup­likasi ke setiap row Parquet.

**Fix:** dua method baru di `MarketOHLCVIngester` — `run_context()` dan
`_run_context_symbol()` — paralel struktur dengan `run()`/`_run_symbol()`
Layer 1, checkpoint namespace terpisah (`bronze_ohlcv_context_daily`, GD
§17.3.1 independence). Default timeframes 1D/1W/1M (`DEFAULT_TIMEFRAMES`)
— identik dengan perilaku AKTUAL job `bronze_ohlcv_daily` hari ini (tidak
ada override `timeframes=` di manapun untuk 5m/15m/1H).

#### GMI-SIL-001 [BLOCKING] — src/silver/ohlcv_processor.py

**Fix:** `run_context(run_date)` — module-level entry point paralel dengan
`run()`, reuse `OHLCVProcessor.process_symbol()`/`write()` **tanpa
modifikasi** (diverifikasi: `_normalize_timestamps()` menerima `tz_hint`
sebagai explicit override yang prioritas di atas market→timezone dict
internal — `market='context'` tidak perlu entry khusus di dict tersebut).
1-pass saja (tidak ada sintesis 4H — tidak ada consumer Layer 2 yang
terdefinisi butuh 4H, dan Bronze context tidak pernah fetch 1H untuk
context anchors juga). Checkpoint namespace: `silver_ohlcv_context`,
terpisah dari `silver_ohlcv_p1`/`silver_ohlcv_4h`.

#### GMI-GLD-001 [P1 HIGH] — src/gold/technical_signals.py — DUA bug, ditemukan berurutan

**Bug 1 (diantisipasi checkpoint):** `_get_latest_vix()` membaca dari
`data/silver/market_ohlcv/index/` — path Layer 1 yang **permanently
empty** sejak ADR-003 (VIX direklasifikasi ke Layer 2 context, Cycle 1).

**Bug 2 (ditemukan BARU lewat empirical testing, TIDAK diantisipasi
checkpoint):** glob pattern memakai **dua** `**` dalam satu path
(`context/**/symbol=VIX/**/*_1D_silver.parquet`). DuckDB `read_parquet()`
menolak ini secara eksplisit — `IO Error: Cannot use multiple '**' in one
path`. Diverifikasi bug ini **sudah ada sebelum ADR-003 sekalipun** —
string `index/**/symbol=VIX/**/...` yang ASLI (pre-fix) menghasilkan error
identik. `except Exception: pass` di sekitar primary query menyembunyikan
KEDUA bug ini sejak fungsi pertama ditulis — primary read tidak pernah
benar-benar berhasil, fallback FRED VIXCLS selalu dipakai secara diam-diam.
Memperbaiki Bug 1 saja tanpa Bug 2 akan menjadi half-fix murni kosmetik
(fungsi tetap selalu jatuh ke fallback, hanya dengan pesan error yang
berbeda) — keduanya diperbaiki bersamaan dalam commit yang sama.

**Fix:** path → `market_ohlcv/context/`; glob → satu `*` pada filename,
nol `**` (struktur write Silver deterministic untuk symbol tunggal, tidak
butuh wildcard direktori sama sekali).

**Ditemukan tapi SENGAJA di luar scope** (RISK-2, RISK-3 di
`KNOWN_RISKS.md`): (a) kelas bug DuckDB double-`**` di atas belum diaudit
di ~20 call site `read_parquet($glob...)` lain; grep string literal tidak
menemukan instance lain, tapi tidak setara audit lengkap. (b) CI Gate G-2
(f-string SQL) menemukan 2 pelanggaran pre-existing di
`src/gold/sector_rotation.py:193` dan `src/gold/views.py:182,196` — TIDAK
disentuh cycle ini (file tidak dimodifikasi, di luar scope Task 9.1,
`views.py` butuh pendekatan identifier-safe quoting yang lebih hati-hati
daripada `$name` binding sederhana). Keduanya didokumentasikan di
`KNOWN_RISKS.md` untuk audit Gold-layer formal berikutnya (R-4).

#### GMI-JR-002 — src/scheduler/job_registry.py

**Fix:** dua job baru — `bronze_ohlcv_context_daily` (`depends_on: []`,
independen dari `bronze_ohlcv_daily`, GD §17.3.1) dan `silver_ohlcv_context`
(`depends_on: ["bronze_ohlcv_context_daily"]`). Keduanya masuk
`DAILY_SEQUENCE` (bukan weekly) — konsisten dengan `GlobalIndexRegimeModule`
(Architecture v2.0 §6.5) yang daily, bukan weekly seperti
`CorrelationModule`/`LeadLagModule`/`ForecastModule`. `JOB_REGISTRY`: 24 →
26 entries. `DAILY_SEQUENCE`: 13 → 15. `WEEKLY_SEQUENCE`: 19 → 21.

**Test baru:**
- `tests/unit/test_market_ingester.py` (BARU — file ini sebelumnya tidak
  ada sama sekali untuk `market_ingester.py`) — 11 test:
  `TestContextSymbolResolution` (3), `TestContextSymbolWrite` (5),
  `TestRunContextEntryPoint` (3)
- `tests/unit/test_ohlcv_processor.py` — `TestRunContextEntryPoint` (6 test baru)
- `tests/unit/test_technical_signals_vix_path.py` (BARU) — 4 test,
  termasuk assertion langsung terhadap DuckDB read yang sebelumnya diam-diam
  selalu gagal (bukan sekadar "tidak crash")
- `tests/integration/test_job_registry_integrity.py` —
  `TestGMIJR002ContextOHLCVWiring` (14 test baru)

**Diverifikasi:** `ast.parse()` OK pada 132 file .py; 1036 passed / 0
failed / 0 error (baseline 1001 → +35); `validate_instruments.py` exit 0
(692 symbols, Layer 1=640, Layer 2=52) — tidak terpengaruh cycle ini.

**Masih di luar scope Cycle 3** (untuk cycle berikutnya, bukan lupa):
`silver_validate`/`quality_validator.py` belum meng-cover Layer 2 symbols
(quality check tetap Layer-1-only untuk saat ini — perluasan butuh
keputusan desain tersendiri tentang bagaimana null/outlier/price-sanity
check berlaku untuk context anchors yang punya karakteristik statistik
berbeda dari saham, mis. VIX yang sering volume=null). `gold_signals`
belum difilter ke `active_ohlcv` (Architecture v2.0 §5.2 — masih memproses
643/640, bukan ~190). `CrossAssetEngine` 4 modul (Correlation/LeadLag/
Forecast/GlobalIndexRegime), `signal_aggregation`, `gold_domain_scores` —
semuanya BELUM diimplementasikan; Cycle 3 ini murni membuka jalan data
mentah untuk mereka.

---

## v1.7.7 — Pre-existing Violations Remediation Wave 2 (Juni 2026)

Dokumen referensi: `audit_preexisting_violations_v1_0.docx`

Total: **5 findings diperbaiki** | **6 file dimodifikasi** | **685 passed / 26 pre-existing failed / 0 error**
(Δ +116 tests dari baseline v1.7.5 — 569 passed)

---

### BCK-SQL-001 [P1 HIGH] — src/backtest/pit_data.py

**6 f-string SQL violations → $name parameterized queries (GD §17.7)**

- Root cause: `PITDataLoader` menggunakan `f"""..."""` untuk semua 6 query (get_ohlcv,
  get_ohlcv_universe, get_macro_series, get_regime, get_signals, get_mtf_score). Variabel
  `symbol`, `trade_date`, `series_id`, path, dan date range semua di-inject langsung ke SQL
  string — PIT date injection memungkinkan lookahead bias jika nilai date dari luar scope.
- Opsi yang dipertimbangkan: (A) `$name` binding untuk semua parameter termasuk IN list,
  (B) IN list via Python-side f-string dengan path+dates parameterized.
  Dipilih A: DuckDB 1.5.4 mendukung list param via `= ANY($symbols)` — fully parameterized.
- Fix: semua 6 query menggunakan `$name` binding; get_ohlcv_universe menggunakan
  `= ANY($symbols)` untuk list param (avoids f-string IN clause).
- Diverifikasi: `ast.parse()` OK; 0 f-string SQL violations via regex scan.
- Test baru: `tests/unit/test_preexisting_violations_v1.py::TestBCKSQL001PITData` (10 tests)
- Tag: `# FIX BCK-SQL-001`

---

### SIL-RPQ-001 [P2 MEDIUM] — Silver layer eager read_parquet

**Eager `pl.read_parquet()` → `pl.scan_parquet().collect()` (Lazy API, GD §10.2)**

Files dimodifikasi:
- `src/silver/ohlcv_processor.py` (2 lokasi: Bronze read loop, 4H synthesis)
- `src/silver/macro_processor.py` (1 lokasi: revision detection)
- `src/silver/fundamental_processor.py` (1 lokasi: get_upcoming_earnings)
- `src/silver/active_symbols.py` (2 lokasi: load(), load_full())

- Root cause: Eager `pl.read_parquet()` membaca seluruh file ke memory sebelum operasi.
  Pada M1 8GB dengan 643 symbol × 7 TF, ini berpotensi OOM di Silver processing loop.
  Polars lazy API dengan `scan_parquet().collect()` memungkinkan columnar pushdown.
- Fix: ganti semua `pl.read_parquet(path)` dengan `pl.scan_parquet(str(path)).collect()`
  di Silver layer (Gold layer exempt — Gold audit terpisah, lihat R-4 KNOWN_RISKS.md).
- Tag: `# FIX SIL-RPQ-001`

---

### UTL-SQL-001 [P2 MEDIUM] — src/utils/delta_reprocessor.py

**2 f-string SQL violations → $name parameterized queries**

- Root cause: `find_stale_symbols()` dan `get_version_summary()` inject glob path dan
  `CURRENT_SILVER_VERSION` string langsung ke f-string SQL.
- Fix: `$glob` dan `$current_version` parameter binding.
- Tag: `# FIX UTL-SQL-001`

---

### BCK-AIO-001 [P2 MEDIUM] + BCK-PIT-001 [P2 MEDIUM] — src/backtest/engine.py

**Non-atomic writes + date.today() PIT violation di `_save_results()`**

- BCK-AIO-001: `result.trades_df.write_parquet()` dan `pl.DataFrame([result.metrics]).write_parquet()`
  diganti `atomic_write_parquet()` — crash di tengah write menghasilkan partial backtest result file.
- BCK-PIT-001: `ts = date.today().isoformat()` di `_save_results()` menyebabkan backtest results
  file ter-stamp dengan tanggal eksekusi, bukan simulation end date — tidak reproducible saat
  di-re-run pada hari berbeda. Fix: `ts = self.config.end_date.isoformat()`.
- Tag: `# FIX BCK-AIO-001`, `# FIX BCK-PIT-001`

---

### CI Gate G-2 Scope Fix — .github/workflows/ci.yml

**Gate G-2 diperluas dari `src/gold/` ke `src/` penuh**

- Root cause: Gate G-2 hanya scan `pathlib.Path('src/gold').rglob('*.py')` — semua
  violations di Silver, Bronze, Backtest, Utils layer lolos CI tanpa terdeteksi.
  Inilah mengapa pre-existing violations dari audit bisa ada selama ini.
- Fix: `pathlib.Path('src').rglob('*.py')` — scan semua layer.
- Tag: `# FIX CI-G2`

---

## v1.7.6 — Pre-existing Violations Remediation Wave 1 (Juni 2026)

Dokumen referensi: `audit_preexisting_violations_v1_0.docx`

Total: **9 findings diperbaiki** | **13 file dimodifikasi** | **1 test file baru** |
**685 passed / 26 pre-existing failed / 0 error**

---

### SIL-SQL-001 [BLOCKING] — src/silver/quality_validator.py

**9 f-string SQL violations → $name parameterized queries + COPY TO restructure**

- Root cause: `_check_null`, `_check_price_sanity`, `_check_coverage`, `_check_gap_detection`,
  `_check_freshness`, `_check_macro_pit`, `_check_adj_integrity`, `_check_vix_circuit_breaker`
  (8 queries) dan `_flag_outliers_in_file` (COPY TO dengan f-string path) — total 9 violations.
  `SILVER_OHLCV_PATH`, `run_date`, dan `vix_glob` di-inject langsung ke f-string SQL.
- Opsi untuk COPY TO: (A) SELECT .pl() + atomic_write_parquet (requires pyarrow via DuckDB .pl()),
  (B) COPY TO via string concatenation — to_path adalah tmpfile kita buat, bukan user input.
  Dipilih B: preserves DuckDB native Parquet writer, zero pyarrow dependency, POSIX-atomic via
  manual tempfile + os.replace. String concat bukan f-string (tidak trigger CI Gate G-2).
- Fix: 8 execute() dikonversi ke parameterized; COPY TO restructure ke string concat + os.replace.
  Semua path via `$glob`, date via `$run_date`, threshold via `$threshold`.
- Diverifikasi: 0 f-string SQL via regex; `test_quality_validator.py::TestOutlierWriteback` 5/5 pass.
- Test: `tests/unit/test_preexisting_violations_v1.py::TestSILSQL001QualityValidator` (11 tests)
- Tag: `# FIX SIL-SQL-001`

---

### SIL-AIO-001 [BLOCKING] — src/silver/ohlcv_processor.py

**Non-atomic Parquet write → `atomic_write_parquet()` (GD §17.7)**

- Root cause: `_write_silver()` langsung panggil `df.write_parquet(out_path)` tanpa temp file.
  Crash mid-write menghasilkan corrupt Silver OHLCV — ini mempengaruhi downstream Gold layer.
- Fix: `atomic_write_parquet(df, out_path, compression="zstd", ...)` via `src/utils/atomic_io.py`.
- Tag: `# FIX SIL-AIO-001`

---

### SIL-AIO-002 [BLOCKING] — src/silver/macro_processor.py

**Non-atomic Parquet write di `_write_silver()` → atomic_write_parquet()**

- Root cause: Macro Silver write tidak atomic — partial macro Silver file mengkorupsi
  regime detection di Gold layer (macro regime baca langsung macro Silver).
- Fix: `atomic_write_parquet()`.
- Tag: `# FIX SIL-AIO-002`

---

### SIL-SQL-002 [P1 HIGH] — src/silver/macro_processor.py

**f-string SQL di `_process_source()` → $glob parameter**

- Root cause: `domain_glob` path di-inject ke `f"""SELECT ... FROM read_parquet('{domain_glob}')"""`.
- Fix: `$glob` parameter binding.
- Tag: `# FIX SIL-SQL-002`

---

### SIL-AIO-003 [P1 HIGH] — src/silver/active_symbols.py

**`shutil.move()` → `os.replace()` untuk atomic rename**

- Root cause: `shutil.move()` adalah pseudo-atomic — pada cross-filesystem operasi, ia melakukan
  copy+delete bukan atomic rename. `os.replace()` adalah POSIX-guaranteed atomic.
  Dua lokasi: `_save()` (line 372) dan `_save_fallback()` (line 407).
- Fix: ganti `shutil.move(str(tmp), str(final))` dengan `os.replace(tmp_path, final)`;
  hapus `import shutil` (tidak digunakan lagi).
- Test: `test_active_symbols.py::TestAS9AtomicWrite::test_save_uses_temp_then_rename` diupdate.
- Tag: `# FIX SIL-AIO-003`

---

### BRZ-AIO-001 [P1 HIGH] — Bronze layer ingesters (3 files)

**Non-atomic writes di Bronze layer → atomic_write_parquet() dengan snappy config**

Files dimodifikasi:
- `src/bronze/base_ingester.py` (2 lokasi: `write()`, `write_macro()`)
- `src/bronze/forex_cache.py` (1 lokasi: `ForexDayCache.save()`)
- `src/bronze/schema_validator.py` (1 lokasi: quarantine write)

- Root cause: Bronze ingesters menulis langsung tanpa temp file. Crash mid-write menghasilkan
  corrupt Bronze file yang kemudian dibaca oleh Silver layer (IncFetchProtocol.scan_last_date).
- Fix: `atomic_write_parquet(df, fname, compression="snappy", compression_level=None,
  row_group_size=100_000, statistics=False, use_pyarrow=False)` — Bronze snappy per GD §7.1.
  `use_pyarrow=False`: Bronze tidak butuh pyarrow; snappy compression level=None (tidak ada level).
- Tag: `# FIX BRZ-AIO-001`

---

### BRZ-SQL-001 [P1 HIGH] — Bronze ingesters (2 files)

**f-string SQL di incremental scan queries**

Files dimodifikasi:
- `src/bronze/eia_ingester.py` (1 lokasi: `_get_last_dates()`)
- `src/bronze/fred_ingester.py` (1 lokasi: `_get_last_dates()`)

- Root cause: `pattern` (glob path) di-inject ke f-string SQL `FROM read_parquet('{pattern}')`.
- Fix: `$glob` parameter binding.
- Tag: `# FIX BRZ-SQL-001`

---

### SIL-SQL-003 [P1 HIGH] + SIL-AIO-004 [P1 HIGH] — Silver fundamental & sentiment

**f-string SQL violations + non-atomic writes di `fundamental_processor.py` dan `sentiment_processor.py`**

Files dimodifikasi:
- `src/silver/fundamental_processor.py`: 2 f-string SQL (process_earnings, process_quotes) +
  2 non-atomic writes (earnings, quotes output)
- `src/silver/sentiment_processor.py`: 1 non-atomic write di `_write()`

- Fix: `$glob` parameterized untuk kedua query; `atomic_write_parquet()` untuk semua writes.
  `use_pyarrow=False` di sentiment processor (original tidak punya pyarrow dep, preserve behavior).
- Tag: `# FIX SIL-SQL-003`, `# FIX SIL-AIO-004`

---

### New Test File

**`tests/unit/test_preexisting_violations_v1.py` (116 tests)**

Comprehensive regression guard untuk semua findings di `audit_preexisting_violations_v1_0.docx`.
Covers: SIL-SQL-001, SIL-AIO-001/002/003/004, BRZ-AIO-001, BRZ-SQL-001, SIL-SQL-002/003,
BCK-SQL-001, SIL-RPQ-001, UTL-SQL-001, BCK-AIO-001, BCK-PIT-001, CI Gate G-2 scope.
`TestGlobalAuditClearance` adalah single gate test untuk semua 14 audit-scope files:
14 × syntax + 14 × f-string SQL + 9 × non-atomic write = 37 parametrized test cases.

---

## v1.7.5 — Gold Layer Audit Remediation (Juni 2026)

Dokumen referensi: `audit_gold_layer_v1_7_4.docx`

Total: **6 BLOCKING findings diperbaiki** | **13 file dimodifikasi** | **1 file baru dibuat** |
**772 passed / 0 failed / 0 error** (Δ +96 tests dari baseline 676)

---

### GLD-001 [BLOCKING] — src/bronze/bea_ingester.py

**BEA NIPA unit-mixing: LINE_FILTER dict + LineDescription filter di `_fetch_nipa()`**

- Root cause: `_fetch_nipa()` menyimpan seluruh 27+ baris respons BEA NIPA table per quarter
  tanpa filter. Table T10106 mengembalikan GDP total (level, billions) sekaligus komponen
  (PCE, GPDI, Government, Net Exports) dan %-change rows dalam satu response — semua tersimpan
  ke Bronze dengan `series_id='real_gdp'` yang sama. HMM `_load_features()` kemudian membaca
  campuran unit yang tidak konsisten → training data corrupted.
- Opsi yang dipertimbangkan: (A) filter per `LineNumber` integer, (B) filter per `LineDescription`
  string. Dipilih B: `LineDescription` lebih stabil lintas BEA API version dan readable.
- Fix: tambahkan `LINE_FILTER: dict[str, str]` constant (3 entries) dan apply filter di loop
  `_fetch_nipa()` — `continue` jika `item['LineDescription'].strip() != target_desc`.
  Series tanpa LINE_FILTER entry → semua rows tersimpan (backward-compatible untuk series baru).
- Diverifikasi: mock response 5 baris → 1 row tersimpan (target LineDescription only).
  Multi-quarter: 4 baris (2 per quarter) → 2 rows (1 per quarter).
- Test baru: `tests/unit/test_bea_ingester_gld001.py` (11 tests)
- Tag: `# FIX GLD-001`

---

### GLD-002 [BLOCKING] — src/gold/macro_regime.py

**DXY score hardcoded 0.5 → formula aktual: `max(0, min(1, (110 - dxy) / 20))`**

- Root cause: `MacroRegimeDetector._classify()` menetapkan `'dxy': 0.5` sebagai score konstan.
  `_load_indicators()` sudah memuat `DEXUSEU` dan mengkonversi ke DXY proxy dengan benar,
  tapi nilai tersebut tidak pernah dibaca oleh `_classify()` — `ind.get('dxy')` tidak dipanggil.
  Dampak: composite_score RISK_ON vs RISK_OFF identik terlepas dari DXY 80 vs DXY 120.
  RISK_OFF tidak terdeteksi saat dollar sangat kuat (krisis EM), menyebabkan false RISK_ON signal.
- Opsi yang dipertimbangkan: (A) formula linear dengan anchor 100 dan range ±10, (B) log-scaling,
  (C) z-score normalize dari historical DXY. Dipilih A: konsisten dengan formula indicator lain
  (vix, yield_spread, cpi, gdp menggunakan linear normalization), auditable, dan no extra data.
- Fix: ekstrak `dxy = ind.get("dxy", 100.0)` (default neutral 100) dan compute
  `"dxy": max(0, min(1, (110 - dxy) / 20))`. DXY=90→1.0, DXY=100→0.5, DXY=110→0.0.
- Diverifikasi: DXY=90 score=1.0 ✓; DXY=100 score=0.5 ✓; DXY=110 score=0.0 ✓;
  DXY=125 capped 0.0 ✓; DXY=80 capped 1.0 ✓. Composite score berbeda antara DXY=90/110 ✓.
- Test baru: `tests/unit/test_macro_regime_gld002.py` (12 tests)
- Tag: `# FIX GLD-002`

---

### GLD-003 [BLOCKING] — 5 Gold layer files

**10 f-string SQL locations → $name parameterized queries (GD §17.7)**

Files dimodifikasi:
- `src/gold/technical_signals.py` (2 × `_get_latest_vix`, 1 × `_process_timeframe`)
- `src/gold/mtf_alignment.py` (1 × `_compute_mtf_alignment`, 1 × `_apply_regime_compatible`)
- `src/gold/screener.py` (1 × `_check_data_freshness`, 1 × `build_watchlist` via table registration, 1 × `_deduplicate_by_cluster`)
- `src/gold/correlation_matrix.py` (1 × `compute_correlation_matrix` + symbol injection)
- `src/gold/hmm_regime.py` (1 × `_load_features`)

- Root cause: semua query menggunakan `f"""SELECT ... FROM read_parquet('{path}')..."""`
  dengan path dan nilai runtime diinterpolasi langsung ke SQL string. Pattern ini:
  (1) melangggar GD §17.7 anti-pattern hard constraint, (2) tidak terdeteksi oleh CI Gate G-2
  lama (grep hanya match f"SELECT pada baris yang sama, triple-quote multi-line lolos),
  (3) menciptakan SQL injection risk pada `correlation_matrix.py` yang menggunakan
  `f"symbol IN ({symbols_sql})"` dengan join dari active_symbols list.
- Fix strategy:
  - `technical_signals`, `mtf_alignment`, `hmm_regime`: replace f-string dengan
    `con.execute(QUERY, {"path": ..., "run_date": ...})` pattern.
  - `screener.build_watchlist`: replace f-string conditional paths + `/dev/null` injection
    dengan Arrow table registration pattern: load optional sources ke Polars DF, register ke
    DuckDB via `con.register()`, kemudian clean SQL dengan `$name` parameters. Helper functions
    `_empty_regime_df()`, `_empty_sector_df()`, `_empty_active_df()` dibuat untuk placeholder
    saat optional data tidak tersedia.
  - `correlation_matrix`: replace `f"symbol IN ({symbols_sql})"` dengan
    `con.register("active_symbols_tbl", active_df.to_arrow())` + `WHERE symbol IN (SELECT ...)`
- Diverifikasi: `test_fstring_sql_absence.py` scan src/gold/ dengan regex window 400 char → 0 violations.
- Test baru: `tests/unit/test_fstring_sql_absence.py` (9 tests)
- Tag: `# FIX GLD-003`

---

### GLD-004 [BLOCKING] — 6 Gold layer files

**Non-atomic Parquet writes → `atomic_write_parquet()` via tempfile + os.replace**

Files dimodifikasi:
- `src/utils/atomic_io.py` (file baru)
- `src/gold/macro_regime.py`, `technical_signals.py`, `mtf_alignment.py`,
  `screener.py`, `correlation_matrix.py`, `sector_rotation.py`

- Root cause: semua Gold layer Parquet writes menggunakan direct `df.write_parquet(path)`.
  Pada M1 8GB RAM dengan 643 symbols × 7 TF pipeline, OOM mid-write meninggalkan partial/corrupt
  Parquet file di target path. Pipeline re-run berikutnya akan membaca file corrupt dan crash
  di downstream job. Pattern ini melanggar Supplementary Design G2 §3.5 yang mewajibkan
  atomic writes untuk Silver/Gold.
- Fix: buat `src/utils/atomic_io.py` dengan `atomic_write_parquet()`:
  1. Buat `NamedTemporaryFile` di direktori parent yang sama dengan target.
  2. `df.write_parquet(tmp_path, **kwargs)`.
  3. `os.replace(tmp_path, path)` — atomic pada POSIX jika same filesystem (guaranteed).
  4. Exception: cleanup tmpfile, re-raise. Target path tidak pernah partial/corrupt.
  Default kwargs: zstd level-3, row_group_size=50_000, statistics=True, use_pyarrow=True.
- Opsi yang ditolak: (A) write_and_verify (baca kembali setelah write) — lebih lambat dan tidak
  mencegah corrupt file tetap ada. (B) custom Parquet writer — tidak perlu, POSIX rename sudah cukup.
- Diverifikasi: mock `write_parquet` raise MemoryError → target path tidak ada ✓; tidak ada
  orphaned .parquet.tmp ✓; os.replace atomic pada same filesystem ✓.
- Test baru: `tests/unit/test_atomic_io.py` (11 tests)
- Tag: `# FIX GLD-004`

---

### GLD-005 [BLOCKING] — src/gold/screener.py

**`TOTAL_INSTRUMENTS = 643` hardcode → `get_loader().count()` dinamis**

- Root cause: `_check_data_freshness()` menggunakan `TOTAL_INSTRUMENTS = 643` literal untuk
  menghitung coverage percentage threshold (95% × 643 = 610.85). Saat universe diperluas ke
  692 (GMI Architecture Extension, Architecture Extension Document v1.0), gate ini diam-diam
  menjadi terlalu longgar: 88% coverage (610/692) akan diterima sebagai ≥95% (610 > 610.85),
  memungkinkan screener berjalan dengan 82 symbols yang tidak ter-cover dari Layer 2 universe.
- Fix: `TOTAL_INSTRUMENTS = get_loader().count()` — import `get_loader` dari instrument_loader,
  tambahkan ke import block screener.py. Count selalu sesuai dengan instruments.yaml saat ini.
- Diverifikasi: mock loader.count()=692, fresh_count=610 → RuntimeError (88.2% < 95%) ✓;
  fresh_count=660 → no raise (95.4% ≥ 95%) ✓. loader.count() dipanggil saat freshness check ✓.
- Test baru: `tests/unit/test_screener_gld005.py` (5 tests)
- Tag: `# FIX GLD-005`

---

### GLD-006 [BLOCKING] — .github/workflows/ci.yml

**CI Gate G-2 blind spot: grep → Python regex dengan 400-char sliding window**

- Root cause: CI Gate G-2 menggunakan `grep -rn 'f"SELECT\|f'"'"'SELECT' src/` yang hanya
  mendeteksi f-string SQL pada baris yang sama. Triple-quote multi-line f-string:
  ```
  f"""
      SELECT ...
  """
  ```
  tidak terdeteksi karena `f"""` dan `SELECT` berada di baris yang berbeda. Akibatnya 10
  violations di Gold layer lolos CI tanpa terdeteksi selama seluruh siklus development v1.7.4.
- Fix: replace grep one-liner dengan Python script yang:
  1. Scan semua `[fF]"""` opener di src/ menggunakan `re.finditer()`.
  2. Ambil 400 char setelah setiap opener (sliding window).
  3. Cek apakah window mengandung SQL keyword (`SELECT`, `FROM read_parquet`, dll).
  4. Report semua violations dengan file path dan line number.
  Semua varian (f""", f''') ter-cover. False positive minimal: f-string tanpa SQL keyword tidak terdeteksi.
- Ditambahkan ke ci.yml: Gate G-4-count (monitoring jumlah test ter-collect dengan BASELINE=676,
  mencegah NEW-4 class collection error terulang).
- Diverifikasi: test matrix 8 positives + 4 negatives → semua correct ✓.
  ci.yml ada dan mengandung Python detection + triple-quote reference ✓.
- Test baru: `tests/unit/test_ci_gate_gld006.py` (12 tests)
- Tag: `# FIX GLD-006`



Dokumen referensi: `audit_v1_7_3_uncovered_findings.docx` (post-implementation audit atas v1.7.3 —
bug yang ditemukan SELAMA implementasi v1.7.3 tapi absen dari `production_readiness_assessment_v1_7_2.docx`
asli; lihat audit §0 untuk metodologi).

Total: **5 dari 7 temuan diperbaiki** (NEW-1, NEW-2, NEW-3, NEW-4, NEW-5) | **1 sudah resolved sebelum
audit ini** (NEW-6/MP-3 — lihat entry v1.7.3) | **1 sengaja dialihkan ke audit formal terpisah** (NEW-7,
BEA NIPA unit-mixing — root cause sudah didokumentasikan di v1.7.3 GAP-1, perbaikan penuh membutuhkan
audit Gold layer formal yang belum pernah dilakukan, lihat "Deferred" di bawah) | **2 BLOCKING + 1 P1 HIGH
+ 1 P2 MEDIUM + 1 P3 LOW** | **13 file dimodifikasi (5 source, 8 test — termasuk 1 file test baru)** |
**Baseline 618 passed / 4 failed / 2 collection error (30 test tidak ter-collect) → 676 passed / 0 failed
/ 0 error** | **Kombinasi coverage modul yang dimodifikasi: 78.25%** (di atas CI gate 70%)

Urutan implementasi mengikuti rekomendasi eksplisit audit §0.2 (Ringkasan Urutan Implementasi):
NEW-1 + NEW-2 (sama-sama menyentuh `job_registry.py`, sama-sama BLOCKING) → NEW-4 (quick win) → NEW-5
(quick win) → NEW-3 (butuh desain paling cermat, lintas market).

---

### NEW-1 [BLOCKING] — `src/scheduler/dependency_guard.py` + `job_registry.py` + `runner.py`
**`python runner.py --job all` selalu `sys.exit(1)` di hari Senin–Sabtu — `DependencyGuard` exact-date matching tidak cocok dengan dependency lintas-cadence**

- Root cause (diverifikasi empiris, mengikuti metodologi audit §2.3 — bukan hanya pembacaan kode):
  `silver_validate` dan `gold_regime` (anggota `DAILY_SEQUENCE`/`PIPELINE_SEQUENCE`) hard-depend pada
  `silver_macro`, yang cadence-nya mingguan (GD §3.3.1) dan TIDAK ada di `DAILY_SEQUENCE` — hanya di
  `WEEKLY_SEQUENCE`, yang sendiri tidak pernah dieksekusi oleh `--job all` (`run_all()` hanya mengiterasi
  `PIPELINE_SEQUENCE`). `DependencyGuard.is_done()` mencari sentinel `silver_macro_{run_date_PERSIS}.done`
  — sentinel dari Minggu lalu tidak pernah cocok untuk pencarian Selasa, Rabu, dst.
- Direproduksi persis seperti audit §2.3: seluruh `job['fn']` di-stub jadi no-op, `run_all()` dijalankan
  lintas 7 hari berturut-turut dimulai dari sebuah hari Minggu (setelah SOP mingguan dijalankan satu kali
  di hari itu) — sebelum fix, 4 dari 7 hari (Senin–Sabtu) crash di job ke-5 dari 13 (`silver_validate`);
  lihat `tests/integration/test_runner_weekly_cadence.py::test_daily_sequence_completes_every_day_of_the_week`
- Opsi yang dipertimbangkan (sesuai audit §2, Opsi A/B/C): Opsi A (staleness-window) dipilih — Opsi B
  (jalankan `silver_macro` setiap hari) melanggar cadence mingguan by design (GD §3.3.1, biaya API FRED/BLS/BEA
  tidak perlu); Opsi C (hapus dependency sepenuhnya) berisiko `silver_validate`/`gold_regime` jalan dengan
  macro data yang benar-benar belum pernah ada sama sekali (tidak hanya stale)
- Fix: `DependencyGuard.is_done_within(job_name, run_date, max_age_days)` — mencari sentinel mundur hingga
  `max_age_days` hari. `check_dependencies()` menerima parameter opsional `stale_tolerance: dict[str, int]`
  — dependency yang TIDAK terdaftar di dict ini tetap memakai exact-date match (`max_age_days=0`), 100%
  backward compatible untuk seluruh job lain yang tidak diubah
- `silver_validate` dan `gold_regime` diberi `"stale_tolerance": {"silver_macro": 7}` — 7 hari = 1 siklus
  mingguan penuh (maksimum 6 hari mundur dari Minggu ke Sabtu berikutnya) + 1 hari buffer
- `runner.py::run_job()` meneruskan `job.get("stale_tolerance")` ke `check_dependencies()`
- **Catatan**: fix ini TIDAK membuat `--job all` berjalan dari instalasi kosong tanpa pernah menjalankan
  SOP Mingguan sama sekali — itu bukan skenario yang valid (operator wajib bootstrap dengan
  `bronze_macro_weekly` + `silver_macro` minimal sekali, sesuai GD §14.4.2). Diverifikasi eksplisit oleh
  `test_no_prior_weekly_run_still_reports_missing_not_crash_signature` — staleness window mempersempit
  false-negative window, bukan mematikan dependency guard
- 8 test baru di `tests/unit/test_dependency_guard.py` (`TestIsDoneWithin`,
  `TestCheckDependenciesStaleTolerance`): exact-match preserved saat `max_age_days=0`, sentinel ditemukan
  dalam window 7-hari untuk tiap hari Minggu→Sabtu, sentinel di luar window tetap dianggap missing, tidak
  pernah mencari maju ke masa depan, `max_age_days<0` raise `ValueError`, dependency lain dalam satu
  `check_dependencies()` call tidak ikut "dilonggarkan" oleh `stale_tolerance` milik dependency lain
- 2 test integrasi baru di `tests/integration/test_runner_weekly_cadence.py` (`TestJobAllAcrossWeek`) —
  reproduksi empiris penuh skenario audit §2.3, lintas 7 hari, mem-verifikasi seluruh 13 job
  `DAILY_SEQUENCE` benar-benar selesai (sentinel ada) setiap hari, bukan hanya "tidak crash"

---

### NEW-2 [BLOCKING] — `src/scheduler/job_registry.py`
**`gold_screener` terkunci permanen — hard-depend pada `silver_fundamental`, yang hard-depend pada `bronze_finnhub` (stub `NotImplementedError` yang disengaja, FIX R-F04)**

- `silver_fundamental.depends_on = ["bronze_finnhub"]`; `bronze_finnhub` sengaja selalu raise
  `NotImplementedError` (belum ada implementasi nyata Finnhub earnings/quotes ingester) — akibatnya
  `silver_fundamental` TIDAK PERNAH bisa menulis sentinel, dan `gold_screener` (yang mencantumkan
  `silver_fundamental` di `depends_on`-nya) TIDAK PERNAH bisa lolos dependency check tanpa `--force`
- GD §5.2.4 sendiri sudah mendesain `earnings_calendar` sebagai `LEFT JOIN` ("data boleh null") —
  `days_to_earnings`/`near_earnings_flag`/`sentiment_score` adalah DATA field opsional (GD §0.3, Interface
  Contract), bukan prasyarat keras untuk screener bisa jalan
- Diverifikasi `gold/screener.py::_enrich_earnings()` SUDAH menangani `silver/fundamental/` yang
  kosong/tidak ada secara graceful (try/except, kolom tetap NULL bila gagal) — **tidak ada perubahan
  diperlukan di `screener.py`** untuk Opsi A; bug murni di `job_registry.py`
- Fix (Opsi A, sesuai rekomendasi audit §3): `"silver_fundamental"` dihapus dari `gold_screener.depends_on`.
  `silver_fundamental` tetap terdaftar penuh di `JOB_REGISTRY` (runnable manual setelah `bronze_finnhub`
  diimplementasikan nyata — Opsi B, item roadmap terpisah, BELUM dikerjakan di sesi ini) dan tetap
  ter-comment-out di `WEEKLY_SEQUENCE` (komentar diperbarui menjelaskan kondisi mengaktifkannya)
- 2 test pre-existing yang gagal sejak v1.7.3 (`test_silver_fundamental_in_sequence`,
  `test_l7_pipeline_sequence_15_or_more_steps` — lihat "Out of Scope" v1.7.3 di bawah) ternyata adalah
  MANIFESTASI LANGSUNG dari NEW-2: keduanya meng-assert asumsi LAMA yang salah (`silver_fundamental`
  seharusnya ada di `PIPELINE_SEQUENCE`, mendorong panjang sequence ke ≥14). Asumsi itu sendiri adalah
  root cause NEW-2 — diupdate untuk mencerminkan kontrak yang benar (Opsi A): `silver_fundamental`
  sengaja absen dari `DAILY_SEQUENCE` sampai `bronze_finnhub` punya implementasi nyata; panjang
  `DAILY_SEQUENCE` yang benar saat ini adalah 13, bukan ≥14
- 2 test baru di `tests/integration/test_job_registry_integrity.py`:
  `test_silver_fundamental_not_required_in_daily_sequence`,
  `test_gold_screener_not_dependent_on_silver_fundamental`
- 2 test integrasi baru di `tests/integration/test_runner_weekly_cadence.py`
  (`TestGoldScreenerNotLocked`): `gold_screener` selesai tanpa `silver_fundamental` pernah dijalankan;
  `silver_fundamental` tetap bisa dijalankan standalone (membuktikan Opsi B tetap memungkinkan tanpa
  perubahan registry lebih lanjut)

---

### NEW-3 [P1 HIGH] — `src/silver/ohlcv_aggregator.py` + `ohlcv_processor.py`
**~67% Silver 4H bar untuk US stocks dan IDX ter-flag `is_clean=False` — `EXPECTED_BARS['4H']=4` flat tidak memperhitungkan bahwa blok UTC-fixed sering hanya overlap sebagian dengan jam sesi trading**

- `_aggregate_4h()` mengelompokkan bar 1H ke blok UTC tetap `[00-03],[04-07],...,[20-23]` (FIX Bug 6,
  sesi sebelumnya — pengelompokan blok sendiri sudah benar dan TIDAK diubah). Bug ada di tahap
  validasi: `is_incomplete_bar = bar_count < EXPECTED_BARS['4H']` memakai ambang flat 4, padahal blok
  yang beririsan sebagian dengan jam buka/tutup sesi pasar secara LEGITIMATE hanya berisi 1-3 sub-bar
  riil — bukan data yang hilang
- 3 opsi dipertimbangkan (sesuai audit §4): Opsi B (re-block berdasarkan sesi, bukan UTC tetap) ditolak —
  mengubah arti `timestamp` kolom output, breaking change untuk semua downstream consumer (MTF alignment,
  dst.) yang mengasumsikan blok UTC tetap. Opsi C (turunkan ambang ke `bar_count < 1`) ditolak — akan
  menutupi gap intraday yang RIIL (gap sungguhan selama jam sesi aktif tidak lagi terdeteksi)
- Fix (Opsi A, direkomendasikan audit): `EXPECTED_BARS` per blok kini DIHITUNG, bukan konstanta — jumlah
  dari 4 jam UTC kandidat blok tersebut yang ber-overlap dengan jam sesi LOKAL bursa instrumen tersebut
  (`MARKET_SESSION_LOCAL`, standard interval-overlap test: `h < close AND h+1 > open`)
- Konversi UTC→lokal memakai IANA timezone database via Polars `dt.convert_time_zone` — mekanisme yang
  SAMA dengan `OHLCVProcessor._normalize_timestamps()` — sehingga DST-aware otomatis: jam UTC mana yang
  termasuk jam sesi bergeser sesuai musim tanpa tabel DST terpisah (diverifikasi empiris: blok UTC[12-15]
  pada 6 Jan 2025/EST menghasilkan expected=2, pada 7 Jul 2025/EDT menghasilkan expected=3 — UTC block_hour
  identik, hasil berbeda murni dari konversi tz yang benar)
- Sesi lokal: `us_stocks`/`index` = NYSE/NASDAQ reguler 09:30–16:00 `America/New_York` (jam sesi resmi,
  bukan buffer "8 jam" konservatif yang dipakai `tvdatafeed_adapter.py` untuk sizing n_bars — itu nilai
  over-estimate untuk fetch sizing, bukan spesifikasi completeness). `idx` = 09:00–14:30 `Asia/Jakarta` —
  REUSE persis window yang sudah ditetapkan FIX TVA-3 (`tvdatafeed_adapter.py`), bukan didefinisikan ulang
  secara independen, untuk menjaga konsistensi internal
- `forex`/`commodity` (`NEAR_24H_MARKETS`) TETAP pakai `EXPECTED_BARS['4H']=4` flat — sesuai audit §4
  ("Forex/commodity yang trading mendekati 24 jam relatif tidak terdampak"), tidak diubah
- Guard tambahan: blok dengan `expected_bars == 0` (sama sekali tidak overlap sesi — mis. blok overnight
  penuh) tidak pernah ditandai `is_incomplete_bar` terlepas dari `bar_count`-nya
- `aggregate_ohlcv()`/`_aggregate_4h()` menerima parameter `market` baru, default `""` — caller yang
  TIDAK mengirim `market` (termasuk seluruh test existing di `test_ohlcv_aggregator.py` yang memanggil
  tanpa argumen ini) mempertahankan PERSIS perilaku flat-4 lama, zero regression. Produksi
  (`OHLCVProcessor.synthesize_4h()`, yang sudah menerima `market` di parameternya) selalu meneruskan
  market instrumen yang sebenarnya
- Memperbaiki kegagalan pre-existing `tests/unit/test_ohlcv_processor.py::TestSynthesize4H::test_clean_input_mostly_clean`
  (gagal sejak v1.7.3 — lihat "Out of Scope" di bawah) tanpa mengubah test itu sendiri sama sekali —
  fixture-nya (`market="us_stocks"`) sudah memanggil dengan benar, fix di sisi aggregator yang
  menyelesaikannya
- 9 test baru di `tests/unit/test_ohlcv_aggregator.py::TestSessionAwareCompleteness`, seluruh angka
  expected_bars diverifikasi empiris terhadap implementasi nyata sebelum dijadikan assertion (bukan
  dihitung manual lalu diasumsikan benar): perilaku default/legacy dipertahankan, overlap sesi
  `us_stocks` menggantikan ambang flat, **gap intraday riil tetap terdeteksi** (regression guard yang
  secara eksplisit membedakan Opsi A dari Opsi C yang ditolak), blok off-session tanpa data tidak
  ter-flag, window sesi `idx`, fallback `forex`/`commodity`, fallback market tak dikenal, **pergeseran
  DST untuk blok UTC yang identik**, input kosong tidak crash

---

### NEW-4 [P2 MEDIUM] — `tests/unit/test_source_adapter.py` + `tests/integration/test_adapter_chain.py`
**Import `DailyBudgetLimiter` dari lokasi lama (`src.bronze.source_adapter`) — class sudah dipindah ke `src.utils.rate_limiter` di sesi sebelumnya (FIX SA-1), memutus collection 30 test di 2 file**

- `from src.bronze.source_adapter import ChainedAdapter, DailyBudgetLimiter, SourceAdapter` →
  `ImportError: cannot import name 'DailyBudgetLimiter'` saat collection — 30 test di kedua file tidak
  pernah dijalankan sama sekali (bukan failed, melainkan collection error — perbedaan yang relevan: CI
  yang hanya memeriksa "tidak ada failed test" tanpa memeriksa jumlah test ter-collect tidak akan
  mendeteksi regresi ini)
- `tests/unit/test_source_adapter.py`: import diperbaiki ke `from src.utils.rate_limiter import
  DailyBudgetLimiter`; `monkeypatch.setattr("src.bronze.source_adapter.date", FakeDate)` di
  `test_resets_on_new_day` juga diperbaiki ke `src.utils.rate_limiter.date` — target lama adalah no-op
  sejak FIX SA-1 (module yang benar-benar membaca `date.today()` sudah pindah)
- `tests/integration/test_adapter_chain.py`: `DailyBudgetLimiter` dihapus dari import top-level —
  sudah tidak terpakai di scope modul (setiap penggunaan aktual sudah punya local import yang benar dari
  `src.utils.rate_limiter` di dalam method masing-masing, sejak FIX SA-1)
- Diverifikasi: 30/30 test ter-collect dan pass di kedua file pasca-fix (sebelumnya: 0 — collection error)

---

### NEW-5 [P3 LOW] — `tests/unit/test_alphavantage_adapter.py`
**`test_parse_dxy` meng-assert proxy `("USD","EUR")` lama — `_parse_pair("DXY")` sekarang sengaja return `("","")` sejak FIX AV-2 (sesi sebelumnya)**

- DXY adalah indeks basket tertimbang (6 mata uang), bukan satu currency pair — FIX AV-2 menghentikan
  proxy via `FX_DAILY(from=USD,to=EUR)` karena hasilnya adalah seri yang secara material berbeda namun
  diberi label DXY. Caller (`AlphaVantageForexAdapter.fetch`) memeriksa sentinel tuple kosong ini dan
  skip AlphaVantage sepenuhnya untuk DXY (fallback ke adapter berikutnya, mis. yfinance `DX-Y.NYB`)
  — perilaku ini SUDAH benar di source code, hanya test yang masih menguji perilaku lama yang sudah dihapus
- Fix: assertion diupdate ke `("","")`, nama method diubah jadi
  `test_parse_dxy_returns_empty_to_signal_skip` agar mencerminkan kontrak saat ini, docstring
  menjelaskan alasan di balik FIX AV-2 untuk pembaca masa depan

---

### NEW-6 [SUDAH RESOLVED sebelum audit ini] — `src/silver/macro_processor.py`
Lihat **MP-3** di entry v1.7.3 di bawah — `_detect_revisions()` `ColumnNotFoundError` silent fallback
sudah diperbaiki dalam sesi v1.7.3, sebelum audit ini ditulis. Audit mengonfirmasi fix tersebut tetap
valid; tidak ada tindakan tambahan di sesi ini.

---

### NEW-7 [DEFERRED] — `src/bronze/bea_ingester.py` (`_fetch_nipa()`)
**BEA NIPA Table 1.1.6 unit-mixing (level vs %-change dalam satu `series_id`) — root cause sudah
didokumentasikan eksplisit di v1.7.3 GAP-1, BUKAN bug baru**

- Tetap sengaja TIDAK diberi alias regime indicator (lihat entry GAP-1 v1.7.3 di bawah: "GDP **sengaja
  TIDAK** diberi alias BEA native `real_gdp`")
- Resolusi penuh membutuhkan audit Bronze layer formal (LineNumber filtering per unit NIPA Table 1.1.6)
  yang belum pernah dilakukan — Bronze dan Gold layer "tidak pernah diaudit formal" tetap menjadi item
  roadmap terbuka, konsisten dengan keputusan GAP-5/GAP-7 di v1.7.3 (audit engagement multi-hari, bukan
  precision fix satu sesi)

---



Dokumen referensi: `production_readiness_assessment_v1_7_2.docx`

Total: **8 dari 10 gap diperbaiki** (GAP-1,2,3,4,6,8,9,10) | **1 bug baru ditemukan & diperbaiki** (MP-3, ditemukan saat menulis test GAP-8) | **19 file dimodifikasi, 9 file baru** | **2 P0 CRITICAL + 3 P1 HIGH + 1 P2 MEDIUM + 2 P3 LOW**

**GAP-5 (Bronze formal audit) dan GAP-7 (Gold formal audit) SENGAJA TIDAK termasuk** dalam siklus ini —
keduanya adalah audit engagement multi-hari (assessment sendiri mengalokasikan 2-3 hari per audit di
Implementation Timeline §8), bukan code fix yang bisa diimplementasikan presisi dalam satu sesi. Lihat
"Deferred" di akhir section ini.

---

### GAP-1 [P0 CRITICAL] — `src/gold/macro_regime.py`
**`_load_indicators()` glob hanya membaca `fred_*_silver.parquet` — BLS/BEA Silver tidak pernah dibaca Gold**

- F-MP-01 (v1.7.2) membuat `bls_*_silver.parquet` / `bea_*_silver.parquet` ada di Silver, tapi
  `_load_indicators()` tidak pernah men-scan file tersebut — half-fix: bug pindah dari "data tidak sampai
  Silver" menjadi "data sampai Silver tapi tidak pernah dibaca Gold"
- Investigasi lebih dalam (bukan sekadar memperlebar glob) menemukan series_id TIDAK konsisten antar
  domain: FRED CPI = `CPIAUCSL`, BLS native CPI = `CUUR0000SA0`, BEA native GDP = `real_gdp` — glob yang
  diperlebar saja TIDAK akan menemukan data baru karena query masih mencari literal `series_id = 'CPIAUCSL'`
- Fix: setiap indicator sekarang punya daftar `(domain, series_id)` candidate berurutan prioritas — `cpi`
  mencoba FRED `CPIAUCSL` dulu, fallback ke BLS native `CUUR0000SA0` (unit sama — index level, aman dicampur)
- GDP **sengaja TIDAK** diberi alias BEA native `real_gdp` — `BEAIngester._fetch_nipa()` menarik semua
  LineNumber NIPA Table 1.1.6 tanpa filter baris, sehingga satu `(series_id, observation_date)` bisa berisi
  multiple value dengan unit berbeda (level vs %-change). Mengalias tanpa resolusi unit berisiko mencampur
  unit ke satu indicator regime — didokumentasikan, dialihkan ke GAP-7 (Gold formal audit)
- DuckDB query di-parameterisasi (`$glob`, `$series_id`, `$run_date`) — sebelumnya f-string interpolation
- 5 test baru di `tests/unit/test_macro_regime.py` (BLS fallback, FRED priority, no-BEA-alias, PIT exclusion, defaults)

---

### GAP-2 [P0 CRITICAL] — `tests/unit/test_quality_validator.py`
**Regresi CI: test masih assert key lama (`ohlcv_null`, dst.) — F-QV-01/02/03 (v1.7.2) merename semua key**

- `test_all_checks_in_result` meng-assert `{ohlcv_null, ohlcv_sanity, ohlcv_outlier, ohlcv_freshness,
  ohlcv_coverage, adj_integrity, macro_pit, vix_circuit}` — set key dari SEBELUM F-QV-01
- v1.7.2 sendiri yang merename key-key ini ke `{null_check, price_sanity, coverage_check, gap_detection,
  outlier_detection, freshness_check, pit_integrity, adj_flag_integrity, vix_circuit}` tapi lupa update test
- Test ini FAIL di setiap CI run sejak v1.7.2 — diverifikasi: `pytest tests/unit/test_quality_validator.py`
  pada repo asli sebelum fix ini menghasilkan `1 failed` persis pada assertion ini
- Fix: assertion diupdate ke 9 key terkini, termasuk `gap_detection` yang baru ada di F-QV-02

---

### GAP-3 [P1 HIGH] — `config/schemas/{fred,bls,bea,imf,eia}_macro.yaml` + 5 Bronze ingester
**6 dari 11 Bronze source tidak punya Schema Registry YAML — SchemaValidator (GD §3.7) tidak pernah aktif**

- Dibuat 5 YAML baru (`fred_macro.yaml`, `bls_macro.yaml`, `bea_macro.yaml`, `imf_weo.yaml`, `eia_oil.yaml`)
  — kolom diverifikasi langsung dari row-construction code tiap ingester (bukan ditebak), termasuk
  perbedaan tipe `observation_date` (FRED: `date`, BLS/BEA/EIA: `string`) dan kolom yang legitimately
  nullable (mis. BEA `value` — `_fetch_nipa()` eksplisit `float(val_str) if val_str else None`)
- `treasury_yield.yaml` dibuat sebagai dokumentasi registry — **bukan** active gate, karena
  `TreasuryIngester` (FIX TI-1/TRES-1) tidak punya write path independen, 100% delegate ke `FREDIngester`
  yang sudah divalidasi `fred_macro.yaml`. Mewiring SchemaValidator kedua di sana tidak akan memvalidasi
  apapun — didokumentasikan eksplisit di file YAML-nya agar tidak ada developer masa depan yang bingung
- SchemaValidator di-wire ke `__init__()` tiap satu dari 5 ingester (FRED, BLS, BEA, IMF, EIA) dan
  divalidasi sebelum `write_macro()` — mismatch → quarantine, mengikuti pola persis `market_ingester.py`
- 22 test baru di `tests/unit/test_bronze_schema_registry_gap3.py`: YAML valid, shape realistis lolos,
  column rename/type-mismatch terdeteksi (gate benar-benar menolak), 5 ingester punya validator terpasang

---

### GAP-4 [P1 HIGH] — `src/silver/quality_validator.py`
**`_check_outliers()` tidak pernah menulis `is_clean=False` ke Silver Parquet meski docstring class mengklaim begitu**

- Method hanya `COUNT(*)` outlier dan log — comment di kode bahkan bilang "flagged is_clean=False in Silver"
  padahal tidak ada write apapun. Class docstring: "Updates is_clean flag in-place" — juga tidak benar untuk method ini
- Fix: outlier (symbol, count) hasil PASS 1 di-writeback per-symbol via `_flag_outliers_in_file()` — re-scan
  satu file Silver 1D kecil per symbol (bukan re-scan 1.6 juta rows), DuckDB `COPY (...) TO tmp (FORMAT
  PARQUET)` lalu `os.replace()` atomic, identik konvensi `ActiveSymbolsResolver._save()` (AS-9)
- Row yang sudah `is_clean=False` dari check lain (mis. `price_sanity`) tidak disentuh — hanya flip True→False,
  tidak pernah sebaliknya; symbol tanpa outlier baru tidak ditulis ulang sama sekali (no-op, no I/O)
- Digabung implementasi dengan GAP-9 (lihat di bawah) karena keduanya menyentuh method yang sama
- 5 test baru di `tests/unit/test_quality_validator.py` (flag tunggal benar, no-write saat clean, idempotent
  re-run, multi-symbol scoping benar, dirty row lain tidak ter-overwrite) — diverifikasi end-to-end dengan
  Parquet fixture asli di disk, bukan mock

---

### GAP-6 [P1 HIGH] — `src/silver/ohlcv_processor.py` + `src/scheduler/job_registry.py`
**`ohlcv_processor.py` tidak punya module-level `run(run_date)` — GD §14.3.2 mensyaratkan tiap modul Silver/Gold punya ini**

- Investigasi menemukan `job_registry.py` justru SUDAH punya `_silver_ohlcv()` wrapper yang bekerja penuh —
  2-pass logic (Bronze raw TF → Silver, lalu Silver 1H → Silver 4H synthesis) di-copy-paste inline, bukan
  delegasi ke modul. Functionally correct, tapi adalah salinan kedua dari logic yang sama
- Risiko: persis pola "half-fix" yang menyebabkan GAP-1 — perbaikan diterapkan ke satu salinan (mis. fix
  MI-1 wildcard glob) tapi salinan lain tidak ikut ter-update jika developer lupa kedua tempatnya
- Fix: `run(run_date)` ditambahkan di `ohlcv_processor.py` sebagai satu-satunya implementasi (memindahkan
  2-pass logic apa adanya, termasuk fix MI-1). `job_registry.py::_silver_ohlcv()` sekarang hanya delegasi
  `from src.silver.ohlcv_processor import run as ohlcv_run; ohlcv_run(run_date)` — pola yang sama persis
  dengan SEMUA wrapper job lain di file tersebut (`_silver_macro`, `_silver_fundamental`, dst.)
- Diverifikasi: `tests/unit/test_ohlcv_processor.py` dan `tests/integration/test_job_registry_integrity.py`
  tetap pass (kecuali 2 kegagalan pre-existing yang tidak terkait, lihat "Out of Scope" di bawah)

---

### GAP-8 [P2 MEDIUM] — `tests/unit/test_macro_processor.py` (BARU)
**File test tidak ada sama sekali — F-MP-01 (process_bls/process_bea) dan F-MP-02 (REVISION_TOLERANCE) tanpa coverage**

- Dibuat dengan 5 test case persis sesuai spesifikasi assessment: `test_process_bls_creates_silver_output`,
  `test_process_bea_creates_silver_output`, `test_run_calls_bls_and_bea`,
  `test_revision_tolerance_no_false_positive`, `test_revision_tolerance_detects_genuine`
- **Bug baru ditemukan saat menulis test ke-5 (genuine revision):** `_detect_revisions()` selalu
  ngeluarin `ColumnNotFoundError: revision_seq_prev` dan silently fallback ke `is_revision=False` untuk
  SETIAP row, SETIAP kali ada prior vintage untuk dibandingkan — lihat **MP-3** di bawah

---

### MP-3 [BARU — ditemukan saat menulis test GAP-8] — `src/silver/macro_processor.py`
**`_detect_revisions()`: `df.join(..., suffix="_prev")` tidak pernah menghasilkan kolom `revision_seq_prev` — exception tertelan, revision detection mati total**

- `prev` (vintage sebelumnya) punya kolom `revision_seq`; `df` (data baru yang sedang diproses) BELUM
  punya kolom `revision_seq` di titik join ini (baru dihitung setelahnya). Polars `suffix="_prev"` HANYA
  diterapkan pada kolom yang collide nama antar kedua frame — karena tidak ada collision untuk
  `revision_seq`, kolom tetap bernama `revision_seq` di hasil join, bukan `revision_seq_prev`
  (`value` TIDAK kena masalah ini karena `df` juga punya kolom `value`, jadi memang collide dan ter-suffix)
- Baris kode `pl.col("revision_seq_prev")...` setelahnya selalu raise `ColumnNotFoundError`, ditangkap oleh
  `except Exception` generik, dan fallback ke `is_revision=False` — F-MP-02's REVISION_TOLERANCE comparison
  **tidak pernah tereksekusi sama sekali** di production untuk symbol manapun yang punya vintage sebelumnya
- Severity: silent — tidak crash, tidak ada di log selain `logger.warning` level yang mudah terlewat;
  ditemukan murni karena test eksplisit menulis assertion `is_revision is True` untuk genuine revision case
- Fix: `prev` di-rename eksplisit (`value`→`value_prev`, `revision_seq`→`revision_seq_prev`) SEBELUM join,
  menghilangkan ketergantungan pada mekanisme auto-suffix Polars yang ambigu
- Diverifikasi: kelima test `test_macro_processor.py` pass, termasuk genuine-revision case yang sebelumnya gagal

---

### GAP-9 [P3 LOW] — `src/silver/quality_validator.py`
**`_check_outliers()`: full Silver 1D scan dua kali (CTE + JOIN) — risiko OOM di M1 8GB budget**

- Query lama: CTE `stats` (scan #1, AVG/STDDEV per symbol) lalu JOIN ke seluruh dataset lagi (scan #2) —
  untuk 643 symbol × 10Y daily (~1.6 juta rows) DuckDB memuat dataset dua kali terhadap budget 3GB (GD §10.2)
- Fix: diganti single-pass DuckDB window function — `AVG/STDDEV OVER (PARTITION BY symbol)` dihitung di
  scan yang sama dengan evaluasi z-score, tidak ada JOIN sama sekali
- Koneksi diganti ke `duckdb_connection()` helper (`src.config.pipeline_config`) agar `memory_limit`/`threads`
  diterapkan konsisten — sebelumnya `duckdb.connect()` polos tanpa budget enforcement
- Diimplementasikan bersamaan dengan GAP-4 (writeback) karena keduanya berada di method yang sama —
  lihat detail lengkap di entry GAP-4 di atas

---

### GAP-10 [P3 LOW] — `KNOWN_RISKS.md` (BARU) + `src/utils/health_reporter.py`
**tvdatafeed: private API reverse-engineered, resiko ToS dan breakage tanpa warning — tidak ada mitigasi runtime**

- `KNOWN_RISKS.md` dibuat — mendokumentasikan resiko tvdatafeed secara eksplisit (blast radius, mitigasi
  yang sudah ada, operator playbook jika `IDX_PARTIAL_FAILURE` muncul, catatan migrasi vendor jangka panjang)
- `health_reporter.py::_check_idx_coverage()` baru — membaca metadata `_symbol`/`_source`/`_ingested_at`
  yang sudah ditulis tiap Bronze file (GD §3.5/§3.6, tidak ada schema baru), menghitung berapa dari 30
  symbol IDX yang benar-benar dari `tvdatafeed` vs fallback `yfinance_jk` vs hilang total hari ini
- Threshold `IDX_COVERAGE_ALERT_THRESHOLD = 5` (persis sesuai assessment): jika fallback+missing > 5 symbol,
  log warning `IDX_PARTIAL_FAILURE` (match istilah IDD §6.3 SOP), set `idx_coverage_alert=True`, tampil di
  terminal report, dan diprioritaskan di Telegram alert (tier sama dengan storage/failed-job alert)
- 5 test baru di `tests/unit/test_health_reporter.py` (full coverage no-alert, degraded triggers alert,
  boundary case tepat di threshold tidak alert, no-Bronze-data graceful, field selalu ada di report)

---

### Deferred — TIDAK termasuk siklus v1.7.3 ini

| Gap | Alasan deferred |
| --- | --- |
| **GAP-5** (Bronze formal audit, 11 ingester modules) | Audit engagement multi-hari per assessment Implementation Timeline §8 (2-3 hari) — bukan code fix presisi. Beberapa temuan arsitektural sudah disurfaced sebagai side-effect dari GAP-1/3/4/6/8 (lihat catatan inline di kode), tapi audit formal lengkap belum dilakukan. |
| **GAP-7** (Gold formal audit, 6 sub-layer modules) | Sama — audit engagement terpisah. GAP-1's investigasi menemukan BEA NIPA multi-row-per-period ambiguity yang eksplisit dialihkan ke sini, bukan ditebak fix-nya. |

### Catatan tambahan dari sesi ini (di luar 10 GAP, ditemukan saat verifikasi regresi)

Hasil `pytest tests/ -q` sebelum dan sesudah perubahan dibandingkan baris-per-baris untuk memastikan zero
regression. 4 kegagalan berikut **sudah ada sebelum sesi ini** dan TIDAK disebabkan oleh perubahan v1.7.3 —
dicatat di sini karena ditemukan, bukan diperbaiki (di luar scope assessment ini):

> **[UPDATE v1.7.4]**: Keempat item di bawah — dan 2 collection error terkait — root-cause-investigated
> secara formal di `audit_v1_7_3_uncovered_findings.docx` dan diperbaiki dalam sesi v1.7.4 sebagai NEW-2,
> NEW-3, NEW-4, dan NEW-5. Lihat entry v1.7.4 di atas untuk detail lengkap masing-masing.

- `tests/unit/test_source_adapter.py` + `tests/integration/test_adapter_chain.py` — collection ERROR:
  `ImportError: cannot import name 'DailyBudgetLimiter' from src.bronze.source_adapter` — **[FIXED v1.7.4, NEW-4]**
- `tests/integration/test_full_system.py::test_l7_pipeline_sequence_15_or_more_steps` dan
  `tests/integration/test_job_registry_integrity.py::test_silver_fundamental_in_sequence` — keduanya
  mengharapkan `silver_fundamental` ada di `PIPELINE_SEQUENCE` (terkait `refactor_plan_sentiment_bronze.docx`)
  — **[FIXED v1.7.4, NEW-2 — asumsi ini ternyata adalah root cause `gold_screener` terkunci permanen]**
- `tests/unit/test_alphavantage_adapter.py::test_parse_dxy` — `_parse_pair` sekarang sengaja menolak DXY
  (lihat log message di kode), tapi test masih mengharapkan parsing lama — **[FIXED v1.7.4, NEW-5]**
- `tests/unit/test_ohlcv_processor.py::TestSynthesize4H::test_clean_input_mostly_clean` — `ohlcv_aggregator`
  menghasilkan hanya 33.3% clean 4H bar dari input 1H yang fully clean, test mengharapkan >= 80%
  — **[FIXED v1.7.4, NEW-3]**

---

### Catatan coverage (`pyproject.toml` `fail_under = 70`)

Kode baru di `ohlcv_processor.py` (GAP-6) dan `health_reporter.py` (GAP-10) awalnya menurunkan combined
coverage 5 modul yang disentuh sesi ini dari **70.92% → 68.48%** — di bawah `fail_under = 70` yang sudah
ada di `pyproject.toml` sebelum sesi ini (bukan threshold baru yang saya tambahkan). Ditambahkan test
lanjutan: `TestRunEntryPoint` (4 test, `test_ohlcv_processor.py`) untuk `ohlcv_processor.run()` end-to-end
dengan Bronze/Silver fixture asli di disk, dan `TestSendTelegramAlert` + 2 test tambahan
(`test_health_reporter.py`) untuk `run()`, `_print_report()`, dan prioritas pesan `send_telegram_alert()`.
Hasil akhir: **80.56%** combined coverage — di atas threshold CI (70%) maupun target Supplementary Design
§10.3 (80%).

---

## v1.7.1 — Precision Audit: ActiveSymbolsResolver (Juni 2026)

Dokumen referensi: `precision_audit_active_symbols.docx`

Total: **12 temuan diperbaiki** | **3 file dimodifikasi** | **2 P0 CRITICAL + 3 P1 HIGH + 4 P2 MEDIUM + 3 NEW**

---

### AS-1 [P0 CRITICAL] — `src/silver/active_symbols.py`
**DuckDB `:name` placeholder → silent NULL di WHERE clause**

- Format `:name` (SQLite convention) tidak disubstitusi secara reliable di DuckDB Python API
- Akibat: `WHERE s.timestamp >= NULL` selalu False → result set kosong → universe 643 tanpa filter
- Fix: ganti semua `:path`, `:run_date`, `:us_dvol`, dst. → `$path`, `$run_date`, `$us_dvol`
- Tambah smoke test eksplisit sebelum main query: `SELECT $v AS x` dengan assert hasilnya bukan NULL

---

### AS-2 [P0 CRITICAL] — `src/silver/active_symbols.py`
**`except Exception` catch-all menulis output seolah resolver berhasil meski query gagal total**

- Fallback `_save(loader.symbol_list(), run_date)` dipanggil untuk SEMUA jenis error termasuk SQL bugs dan OOM
- DependencyGuard tidak mendeteksi kegagalan → Gold layer berjalan dengan 643 simbol tanpa filter likuiditas
- Fix: pisahkan dua kondisi semantik berbeda:
  - Silver not ready → fallback legitimate, `is_fallback=True` di output
  - Query/runtime error → fail-fast, exception propagate ke runner.py → sentinel tidak ditulis

---

### AS-3 [P1 HIGH] — `src/silver/active_symbols.py`
**`AVG(dollar_volume)` dihitung dari 45 hari kalender, bukan 20 trading day eksplisit**

- Jumlah observasi per symbol tidak setara: US stocks ~31 hari, IDX ~28 hari, forex 45 hari
- Threshold `dollar_volume_20d` tidak memiliki semantik konsisten antar pasar
- Fix: tambahkan CTE `ranked_clean` dengan `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_date DESC)`
- `AVG(dollar_volume) FILTER (WHERE rn <= 20)` — tepat 20 trading day terbaru

---

### AS-4 [P1 HIGH] — `src/silver/active_symbols.py`
**Dirty rows (`is_clean=False`) mempengaruhi `dollar_volume_20d` dan `last_close`**

- `AVG(dollar_volume)` dan `LAST(last_close)` dihitung dari semua rows termasuk outlier volume Z>4
- Satu bar dengan volume spike dalam 45 hari dapat menaikkan `dollar_volume_20d` secara signifikan
- Fix: pindahkan `AND s.is_clean = TRUE` ke level `ohlcv` CTE (sumber) — affects ALL downstream CTEs
- `COUNT(*)` di CTE sekarang menghitung hanya clean rows secara konsisten

---

### AS-5 [P1 HIGH] — `src/silver/active_symbols.py`
**`LIMIT 200` global memotong forex/commodity/index yang seharusnya always-in**

- Forex (20) + commodity (3) + index (2) = 25 instrumen dengan `dollar_volume_20d = 0`
- Selalu berada di posisi terbawah `ORDER BY dollar_volume_20d DESC` → dipotong jika `us_stocks + idx >= 200`
- Fix: UNION policy — `always_in CTE` (no LIMIT) + `screened CTE` (LIMIT 175, headroom untuk 25 always-in)
- Total maksimum tetap ~200; always-in markets dijamin masuk tanpa tergantung kondisi screened

---

### AS-6 [P2 MEDIUM] — `src/silver/active_symbols.py`
**Output schema hanya `symbol + resolved_date` — tidak cukup untuk audit institutional**

- Perubahan universe tidak dapat dijelaskan tanpa rerun query dari Silver data historis
- Fix: output schema diperkaya: `market`, `dollar_volume_20d`, `clean_days`, `last_close`,
  `eligibility_reason`, `resolved_date`, `resolver_version`, `unknown_market_count`, `is_fallback`
- Tambah method `load_full()` untuk mengakses full DataFrame dengan semua kolom audit

---

### AS-7 [P2 MEDIUM] — `src/silver/active_symbols.py`
**DuckDB connection tidak ditutup eksplisit → resource leak pada scheduler long-running**

- `duckdb.connect()` tanpa context manager → connection hidup sampai GC pada M1 8GB terbatas
- Fix: `with duckdb.connect() as con:` — connection auto-closed setelah block selesai

---

### AS-8 [P2 MEDIUM] — `src/silver/active_symbols.py`
**`OUTPUT_PATH`, `memory_limit`, `threads` hardcoded — mengabaikan konfigurasi global**

- `memory_limit='2GB'` berbeda dari GD §10.2 (`3GB`); `OUTPUT_PATH` mengabaikan `PIPELINE_DATA_ROOT`
- Fix: `OUTPUT_PATH` dibaca dari `PIPELINE_DATA_ROOT` env var (fallback ke `get_config()`)
- `memory_limit` dan `threads` dari `get_config().duckdb_memory_limit_gb` dan `.duckdb_threads`

---

### AS-9 [P2 MEDIUM] — `src/silver/active_symbols.py`
**`write_parquet()` langsung ke path final → partial write risk jika crash saat write**

- File corrupt dengan nama final tersisa di disk → downstream job gagal dengan Parquet read error
- Fix: tulis ke `tempfile.NamedTemporaryFile` di direktori yang sama, lalu `shutil.move()` atomic
- Cleanup `tmp_path.unlink(missing_ok=True)` di except block untuk mencegah orphaned `.tmp` files

---

### AS-10 [NEW] — `src/silver/active_symbols.py`
**Simbol unknown market hilang diam-diam dari universe tanpa log apapun**

- Simbol dalam Silver OHLCV yang tidak ada di InstrumentLoader mendapat `market=NULL` dari LEFT JOIN
- NULL symbols tidak masuk kondisi manapun → dikecualikan tanpa log atau audit trail
- Fix: `m.market IS NOT NULL` guard di `ohlcv` CTE; `_audit_unknown_markets()` method
- Log WARNING dengan daftar orphan symbols; `unknown_market_count` disimpan di output metadata

---

### AS-11 [NEW] — `src/silver/active_symbols.py`
**`hive_partitioning=True` berisiko conflict dengan kolom data Silver (kolom duplikat silent)**

- Silver partition key `symbol=AAPL/` dengan `hive_partitioning=True` dapat overwrite kolom data `symbol`
- Menghasilkan data salah secara silent — bukan error (Supp. Design v1.1 G2 catatan)
- Fix: `hive_partitioning=false` di query — konsisten dengan konvensi Silver layer

---

### AS-12 [NEW] — `src/silver/active_symbols.py`
**Section 7 dokumen basis (sketch query) tidak lengkap — implementasi verbatim salah**

- Section 7 sketch: tidak ada WHERE threshold, tidak ada UNION policy, tidak ada LIMIT
- Implementasi sketch Section 7 menghasilkan 643 simbol tanpa screening
- Fix: query final dari audit Section 8 diimplementasikan — mencakup semua perbaikan AS-1..AS-10

---

*Tests: `tests/unit/test_active_symbols.py` dan `tests/integration/test_active_symbols_integration.py` diperbarui untuk mencakup semua 12 temuan.*

*Semua perubahan mengikuti prinsip Layer Independence (GD §17.2) dan Interface Contract (GD §17.6).*

---

## v1.7 — Precision Audit Fixes (June 2026)

Dokumen referensi: `precision_audit_bronze_silver_v1_6.docx`

Total: **11 bug diperbaiki** | **11 file dimodifikasi** | **3 CRITICAL + 5 HIGH + 3 MEDIUM**

32 bug sebelumnya (v1.4–v1.6) terkonfirmasi verified dalam audit dan tidak berubah di v1.7.

---

### QV-1 [CRITICAL] — `src/silver/quality_validator.py`
**`class QualityValidator:` tidak pernah dideklarasikan — NameError saat runtime**

- `QualityGateError` body tidak ditutup — method `__init__` QualityValidator masuk ke dalam `QualityGateError`, overwrite `__init__(failed_checks)` dengan `__init__()` tanpa argumen
- `QualityValidator` tidak pernah terdefinisi → `run()` crash dengan NameError setiap kali `silver_validate` dijalankan
- Fix: tambahkan `class QualityValidator:` sebagai class terpisah, tutup `QualityGateError` dengan benar

---

### EIA-4 [CRITICAL] — `src/bronze/eia_ingester.py`
**`_build_last_known_cache()` key mismatch → incremental fetch tidak pernah aktif**

- Cache dikunci oleh `series_id` (e.g. `'PET.WCRSTUS1.W'`) tetapi lookup menggunakan `spec['name']` (e.g. `'us_crude_stocks'`)
- `last_known_cache.get(spec['name'])` selalu return `None` → setiap Rabu fetch 5-year full history
- Fix: ganti `last_known_cache.get(spec['name'])` → `last_known_cache.get(spec['id'])`

---

### MI-1 [CRITICAL] — `src/scheduler/job_registry.py`
**Pass 1 `_silver_ohlcv()` hardcode `source=yfinance/` → IDX, forex, Polygon data tidak pernah masuk Silver**

- Pattern `BRONZE_OHLCV_PATH / inst.market / f'source=yfinance/symbol={inst.symbol}/**/*.parquet'` hanya match `YFinanceAdapter`
- IDX (tvdatafeed, source=tvdatafeed/), forex (source=yfinance_forex/), US fallback (source=polygon/) semua missed
- Fix: ganti dengan wildcard `** / f'symbol={inst.symbol}' / '**' / '*.parquet'` yang mencakup semua source

---

### FH-3 [HIGH] — `src/bronze/finnhub_ingester.py`
**`get_days_to_earnings()` glob masih path lama `earnings_calendar/finnhub/` → `days_to_earnings` selalu None**

- FH-1 membenarkan write path ke Hive `earnings_calendar/source=finnhub/...` tetapi read glob masih `earnings_calendar/finnhub/...`
- Glob tidak pernah match → Gold Screener `days_to_earnings` dan `near_earnings_flag` selalu None
- Fix: ganti glob ke wildcard `earnings_calendar / '**' / '*.parquet'` tanpa asumsi source= prefix

---

### MP-1 [HIGH] — `src/silver/macro_processor.py`
**`process_treasury()` membaca `data/bronze/bond/treasury/` (path kosong) → Treasury Silver selalu no-op**

- `TreasuryIngester` mendelegasi ke `FREDIngester` yang menulis ke `data/bronze/macro/fred/monetary_policy/`
- `process_treasury()` membaca `bond/treasury/` yang tidak pernah ditulis siapapun
- Data Treasury (DGS series, T10Y2Y) sudah diproses oleh `process_fred()` — tidak ada data loss
- Fix: `process_treasury()` dikonversi ke no-op dengan `logger.debug()`; hapus `_process_domain()` call yang berujung ke path kosong

---

### YF-1 [HIGH] — `src/bronze/yfinance_adapter.py`
**`'4H': '1h'` dead code di `_INTERVAL_MAP` setelah v1.5 refactoring**

- Bronze tidak lagi fetch 4H setelah v1.5 (GD §3.1, §17.7); 4H disintesis dari Silver 1H
- Entry `'4H': '1h'` adalah silent mislabeling risk: jika dipanggil, fetch 1H data berlabel 4H tanpa error
- Fix: hapus `'4H'` dari `_INTERVAL_MAP`; tambahkan explicit `ValueError` guard di `fetch()` dengan pesan actionable

---

### SA-1 [HIGH] — `src/bronze/source_adapter.py`
**`DailyBudgetLimiter` duplikat (tidak thread-safe) dan `AV_LIMITER` dead code**

- `source_adapter.py` mendefinisikan `DailyBudgetLimiter` tanpa `threading.Lock` — tidak thread-safe
- `rate_limiter.py` sudah punya implementasi thread-safe yang digunakan `alphavantage_adapter.py` via `SourceLimiters.alphavantage`
- `AV_LIMITER = DailyBudgetLimiter(25)` tidak pernah diimport atau digunakan di manapun
- Fix: hapus `DailyBudgetLimiter` class dan `AV_LIMITER` dari `source_adapter.py`; konsolidasikan ke `rate_limiter.py`

---

### FF-1 [HIGH] — `src/silver/fundamental_processor.py`
**f-string SQL injection di `get_days_to_earnings()` — inkonsisten dengan FH-2/IMF-2 pattern**

- `WHERE symbol = '{symbol}' AND run_date = '{run_date}'` — f-string langsung di SQL
- Risk jika symbol mengandung karakter khusus; inconsistent dengan pola parameterized yang sudah ada
- Fix: ganti f-string dengan DuckDB `?` placeholder binding: `con.execute("... WHERE symbol = ?", [pattern, symbol, str(run_date)])`

---

### POL-6 [MEDIUM] — `src/bronze/polygon_adapter.py`
**`import pandas as pd` di dalam loop body — overhead lookup tiap iterasi, sulit di-mock**

- Dua `import pandas as pd` di dalam `fetch()` body: satu di loop, satu di return statement
- Python men-cache module tapi dict lookup overhead tiap call; lebih penting: sulit di-mock untuk testing
- Fix: pindahkan ke module-level top-of-file import (setelah `import polars as pl`)

---

### TI-1 [MEDIUM] — `src/bronze/treasury_ingester.py`
**Unnecessary inheritance dari `BronzeIngester`**

- `TreasuryIngester(BronzeIngester)` tidak pernah memanggil `self.write()` atau `self.write_macro()` langsung
- Semua write dilakukan oleh `FREDIngester` yang di-delegate; inheritance menambahkan method yang tidak digunakan
- Fix: ganti `class TreasuryIngester(BronzeIngester):` → `class TreasuryIngester:` (plain class)

---

### OP-1 [MEDIUM] — `src/silver/ohlcv_processor.py`
**Sort condition `'symbol' in df.columns` selalu False setelah `_normalize_columns()`**

- `_normalize_columns()` drop kolom `_symbol` dan rename — setelah pipe, outer `df` masih pre-pipe schema
- Conditional `if 'symbol' in df.columns` selalu False → `sort(['symbol','timestamp'])` tidak pernah dieksekusi
- `process_symbol()` selalu single-symbol — sort by `['timestamp']` saja sudah benar dan tidak misleading
- Fix: hapus conditional, gunakan `sort(['timestamp'])` langsung

---

*Semua perubahan mengikuti prinsip Separation of Concerns (GD §0), Layer Independence (GD §17.2), dan Interface Contract (GD §17.6).*

*Grand Design v1.2 > Supplementary Design v1.1 > IDD v1.0 — hierarki tetap berlaku.*

---



Dokumen referensi: `audit_perbaikan_market_data_infrastructure_v1_1.docx`

Total: **13 file dimodifikasi**, **28 bug/gap diperbaiki**

---

### B-F01 [CRITICAL] — `src/bronze/market_ingester.py`
**`_run_symbol()`: variabel `source` tidak terdefinisi → NameError runtime**

- Tambah method `_primary_source_for(inst)` yang menentukan primary source per asset class
- `_run_symbol()` menggunakan `primary_src` dari method baru — bukan variabel `source` yang tidak ada
- Signature `_fetch()` dibersihkan (hapus parameter `source` yang tidak digunakan)

---

### B-F02 [HIGH] — `src/bronze/market_ingester.py` + `src/silver/ohlcv_processor.py`
**`data_source` di Silver hardcode `'yfinance'` — tidak mencerminkan adapter aktual**

- `_run_symbol()`: baca `actual_source` dari `df["_source"][0]` setelah ChainedAdapter fetch
- `_normalize_columns()`: TIDAK drop `_source` — capture sebelum drop Bronze metadata
- `_add_metadata()`: terima parameter `actual_source`; gunakan sebagai `data_source` kolom

---

### B-F07 [MEDIUM] — `src/bronze/market_ingester.py`
**`ForexDayCache.save()` tidak pernah dipanggil setelah primary fetch sukses**

- `_run_symbol()`: panggil `ForexDayCache().save()` setelah primary fetch forex berhasil
- Wrapped dalam try/except agar cache failure tidak menghentikan ingestion

---

### S-F01 [CRITICAL] — `src/silver/quality_validator.py`
**Critical check failures hanya di-log sebagai warning — Gold layer tidak terblokir**

- Tambah `QualityGateError(RuntimeError)` exception class
- Definisikan `CRITICAL_CHECKS` dan `WARNING_CHECKS` set secara eksplisit
- `run()`: raise `QualityGateError` jika critical check gagal → DependencyGuard tidak menulis `.done` sentinel → Gold layer otomatis terblokir

---

### S-F02 [CRITICAL] — `src/silver/macro_processor.py` + `src/silver/fundamental_processor.py`
**Data macro dan earnings tidak difilter `release_date <= run_date` → lookahead bias**

- `macro_processor._process_domain()`: filter `release_date <= run_date` sebelum write Silver; fallback ke `observation_date` jika kolom release_date tidak ada
- `fundamental_processor._process_earnings()`: filter `fetched_date <= run_date` sebelum dedup

---

### S-F03 [HIGH] — `src/silver/ohlcv_processor.py`
**Tidak ada `.sort()` sebelum `_add_derived_fields()` → shift/ewm pada data tidak terurut**

- `process_symbol()`: tambah `.sort(["symbol", "timestamp"])` dalam chain sebelum `_add_derived_fields()`

---

### S-F04 [HIGH] — `src/silver/ohlcv_processor.py`
**`staleness` di-drop di `_normalize_columns()` sebelum `_flag_is_clean()` membutuhkannya**

- `_normalize_columns()`: TIDAK drop `staleness` — hanya drop Bronze audit metadata
- `_add_metadata()`: drop `staleness` DI SINI, setelah `_flag_is_clean()` selesai

---

### G-F01 [CRITICAL] — `src/gold/screener.py`
**`dollar_volume_20d` dibaca dari `sector_regime_weights.parquet` yang tidak memiliki kolom itu**

- Tambah CTE `active` yang membaca dari `data/silver/active_symbols/active_{date}.parquet`
- LEFT JOIN `active a ON m.symbol = a.symbol` menggantikan `s.dollar_volume_20d`
- WHERE clause diupdate ke `COALESCE(a.dollar_volume_20d, 1e9)`

---

### G-F03 [HIGH] — `src/gold/mtf_alignment.py`
**ATR diambil dari TF pertama yang punya `atr_14` — bukan 1H**

- Ganti `next(df for df in tf_dfs if "atr_14" in df.columns)` dengan eksplisit filter `df["timeframe"][0] == "1H"`
- Fallback ke TF apapun dengan log warning jika 1H tidak tersedia

---

### G-F04 [HIGH] — `src/gold/mtf_alignment.py`
**`reward_risk_ratio = 1.5*ATR / 0.5*ATR = 3.0` selalu konstan**

- Ganti dengan RRR berbasis level harga aktual: `(1.5*ATR) / (1.25*ATR)` — entry -0.25*ATR dari close, stop -1.5*ATR dari close
- Formula sekarang mencerminkan actual risk distance, bukan ATR ratio

---

### G-F06 [HIGH] — `src/gold/hmm_regime.py`
**`StandardScaler` tidak di-fit dan tidak di-persist → klasifikasi dengan skala berbeda saat inference**

- `fit()`: fit `StandardScaler` pada training data; `fit_transform` digunakan untuk training
- `_save_model()`: persist `{"model": ..., "scaler": ...}` sebagai dict dalam satu pickle file
- `_load_model()`: restore scaler; backward-compat untuk format lama (model-only pickle)
- `classify()`: apply `scaler.transform()` sebelum `model.predict()`
- `__init__()`: inisialisasi `self._scaler = None`

---

### GD-F01 [HIGH] — `src/gold/technical_signals.py`
**VIX Spike Guard membaca dari Silver macro FRED (delay hari) bukan Silver OHLCV (harian)**

- Primary: baca VIX close dari `data/silver/market_ohlcv/index/**/symbol=VIX/**/*_1D_silver.parquet`
- Fallback: Silver macro FRED `VIXCLS` jika OHLCV tidak tersedia, dengan debug log

---

### GD-F02 [HIGH] — `src/gold/screener.py`
**Tidak ada Data Freshness Gate (GD §15.1) sebelum screener berjalan**

- Tambah `_check_data_freshness(run_date)` — dipanggil di awal `run()`
- Query Silver 1D: hitung distinct symbols dengan `is_clean=TRUE` dalam 3 hari terakhir
- Raise `RuntimeError` jika coverage < 95% dari 643 instrumen
- Skip gate dengan warning jika Silver belum ada (phase awal)

---

### GD-F03 [HIGH] — `src/gold/correlation_matrix.py`
**Tidak ada warning jika `active_symbols` > 250 (batas aman RAM M1 8GB)**

- Tambah `MAX_SYMBOLS_RAM_SAFE = 250` guard di `compute_correlation_matrix()`
- Log warning dengan detail matrix size dan rekomendasi tighten threshold

---

### GD-F04 [HIGH] — `src/scheduler/job_registry.py`
**`silver_active_symbols` ada di registry tapi dependency chain tidak konsisten dengan IDD §7**

- Dependency sudah benar di JOB_REGISTRY entry; diperkuat via DAILY_SEQUENCE order

---

### GD-F08 [MEDIUM] — `src/bronze/base_ingester.py`
**GD §3.1 Idempotency: `write()` selalu menulis file baru tanpa cek existing**

- `write()`: scan `path.glob(f"{symbol}_raw_{date_prefix}*.parquet")` sebelum tulis
- Jika file hari yang sama sudah ada → return `None` (skip), log debug
- Return type diubah ke `Optional[Path]`

---

### R-F02 [HIGH] — `src/runner.py`
**`run_all(force=True)` tidak berpengaruh — loop memanggil `run_job(force=False)` hardcoded**

- `run_all()`: pass `force=force` ke setiap `run_job()` dalam loop

---

### R-F03 [HIGH] — `src/scheduler/job_registry.py`
**`PIPELINE_SEQUENCE` tunggal mencampur daily dan weekly jobs → `silver_macro` gagal dependency setiap hari**

- Pisahkan `DAILY_SEQUENCE` (daily cadence) dan `WEEKLY_SEQUENCE` (Minggu + daily)
- `silver_macro` HANYA ada di `WEEKLY_SEQUENCE`
- `PIPELINE_SEQUENCE = DAILY_SEQUENCE` (backward-compat alias)
- `runner.py`: import `DAILY_SEQUENCE` dan `WEEKLY_SEQUENCE`

---

### R-F04 [HIGH] — `src/scheduler/job_registry.py`
**`_bronze_finnhub()` stub hanya `logger.info()` — silent success tanpa data**

- Ubah stub ke `raise NotImplementedError(...)` dengan pesan actionable
- Dependency guard tidak menulis sentinel → `silver_fundamental` tidak akan jalan

---

*Semua perubahan mengikuti prinsip Separation of Concerns (GD §0), Layer Independence (GD §17.2), dan Interface Contract (GD §17.6).*

*Grand Design v1.2 > Supplementary Design v1.1 > IDD v1.0 — hierarki tetap berlaku.*
