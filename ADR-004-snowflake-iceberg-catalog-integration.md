# ADR-004: Snowflake–Iceberg Catalog Integration Strategy — REST + External Volume Credentials vs Vended Credentials

## Status
Accepted

## Kontekst

Faza 3 zakończona: 6 tabel Iceberg (`dim_customer`, `dim_product`, `fact_orders`,
`fact_order_items`, `fact_payments`, `fact_order_status_history`) istnieje i ma
realne dane, zarządzane przez Glue Data Catalog (baza `portfolio_ecommerce`) w S3
(`tp-portfolio-ecommerce-raw`). Faza 4 podłącza Snowflake do tych samych tabel
(odczyt natywny, bez kopiowania danych) jako warstwa warehousingu dla dbt.

Snowflake oferuje dwie ścieżki integracji z AWS Glue jako katalogiem Iceberga:

- **Klasyczna integracja (`CATALOG_SOURCE = GLUE`)** — starszy mechanizm,
  bezpośrednie wywołania Glue API (`GetTable`/`GetTables`).
- **REST (`CATALOG_SOURCE = ICEBERG_REST`, `CATALOG_API_TYPE = AWS_GLUE`)** —
  nowszy mechanizm przez Iceberg REST endpoint Glue (SigV4), obecnie oficjalnie
  rekomendowany przez Snowflake ("we recommend that you instead create a catalog
  integration for the AWS Glue Iceberg REST endpoint").

W ścieżce REST dochodzi druga decyzja — `ACCESS_DELEGATION_MODE`:

- `VENDED_CREDENTIALS` — Snowflake dostaje dostęp do S3 pośrednio, przez
  poświadczenia wydawane przez katalog; dla AWS Glue w praktyce wiąże się
  z warstwą AWS Lake Formation jako mechanizmem zarządzania dostępem.
- `EXTERNAL_VOLUME_CREDENTIALS` (domyślne) — Snowflake łączy się do S3
  bezpośrednio przez rolę IAM wskazaną w external volume, koncepcyjnie
  identycznie do tego, jak Spark/Iceberg już łączą się z S3 w tym projekcie.

Projekt nie ma (i nie planował) Lake Formation w swojej infrastrukturze —
obecny model dostępu to prosty IAM user/rola z policy na S3 + Glue.

## Decyzja

1. **Catalog integration typu REST** (`CATALOG_SOURCE = ICEBERG_REST`,
   `CATALOG_API_TYPE = AWS_GLUE`, `REST_AUTHENTICATION TYPE = SIGV4`) —
   zgodność z aktualną rekomendacją Snowflake, zamiast klasycznej integracji
   `GLUE`, którą Snowflake już oznacza jako do wygaszenia.

2. **`ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS`**, nie
   `VENDED_CREDENTIALS`. Unikamy wprowadzania Lake Formation jako nowej,
   równoległej warstwy zarządzania dostępem, obok już istniejącego,
   działającego modelu IAM-role-per-service. Mniej ruchomych części,
   bez utraty żadnej funkcjonalności istotnej dla tego projektu.

3. **Catalog-linked database** (`CREATE DATABASE ... LINKED_CATALOG = (...)`)
   zamiast ręcznej deklaracji `CREATE ICEBERG TABLE` per tabela — automatyczne
   odkrycie istniejących namespace'ów/tabel z Glue Catalog, spójne z tym, że
   tabele już istnieją (Faza 3), nie są tworzone od zera przez Snowflake.

4. Nowa rola IAM (nie rozszerzenie istniejącego usera `portfolio-pipeline`)
   z trust policy pozwalającą Snowflake ją `AssumeRole` (external ID) —
   oddzielna tożsamość per-konsument katalogu, zgodnie z zasadą najmniejszych
   uprawnień już stosowaną w projekcie (IAM user bez dostępu do konsoli).

## Rozważane alternatywy

**Alternatywa 1 — Klasyczna integracja `CATALOG_SOURCE = GLUE`.**
Odrzucone, bo:
- Snowflake jawnie rekomenduje REST jako następcę i wskazuje dodatkowe
  możliwości (m.in. vended credentials) niedostępne w starszej ścieżce
- brak uzasadnienia biznesowego/edukacyjnego, by uczyć się mechanizmu
  oznaczonego jako legacy, skoro REST jest tak samo dostępny od startu

**Alternatywa 2 — `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` z Lake Formation.**
Odrzucone dla tego etapu projektu, bo:
- wymagałoby wprowadzenia całej nowej usługi (Lake Formation) tylko po to,
  by zmienić mechanizm przekazywania poświadczeń, bez realnej korzyści
  funkcjonalnej przy obecnej skali (jeden konsument katalogu — Snowflake)
- zwiększa powierzchnię do nauki naraz z resztą Fazy 4 (RBAC, masking, RLS),
  rozmywając priorytety tej fazy
Zostałoby to dobrym wyborem, gdyby projekt docelowo miał wielu konsumentów
katalogu (Snowflake + Athena + Spark + inny silnik) wymagających spójnej,
scentralizowanej polityki dostępu na poziomie tabeli/kolumny w jednym miejscu
— dokładnie do tego Lake Formation jest projektowany.

## Konsekwencje

**Pozytywne:**
- Zgodność z aktualnym kierunkiem rozwoju Snowflake (REST), bez uczenia się
  ścieżki do wygaszenia
- Spójny mentalnie z resztą projektu model dostępu (IAM role + policy),
  bez nowej, równoległej warstwy autoryzacji
- Catalog-linked database eliminuje ręczne powtarzanie definicji 6 tabel

**Negatywne / trade-offy:**
- Brak w projekcie demonstracji Lake Formation — świadoma luka do
  ewentualnego wspomnienia na rozmowie rekrutacyjnej jako "next step",
  nie jako coś przeoczonego
- External volume to dodatkowy obiekt Snowflake do utrzymania (obok catalog
  integration), którego `VENDED_CREDENTIALS` by nie wymagał

## Powiązane

- ADR-002 (Bronze ingestion strategy) — ten sam Glue Data Catalog i bucket S3
  jako punkt odniesienia dla warstwy Iceberg
- `PROGRESS_LOG.md`, sekcja 12 (Faza 4 — start)
