# ADR-005: Nazewnictwo warstw — `dim_`/`fct_` tylko dla martów; rename tabel Iceberg

## Status
Accepted. Wdrożone i zweryfikowane: rename w Glue (liczby wierszy i
`metadata_location` identyczne przed/po), aktualizacja kodu, ponowne założenie
masking/RLS w Snowflake oraz `dbt run`/`dbt test` (potwierdzone przez użytkownika,
bez zachowanego outputu).

## Kontekst

Faza 3 nazwała 6 tabel Iceberg od razu wg Kimballa: `dim_customer`, `dim_product`
(pełne SCD Type 2), `fact_orders`, `fact_order_items`, `fact_payments`,
`fact_order_status_history`. Fizycznie leżą przed warehouse'em i przed dbt, czyli
w warstwie Bronze/Silver. W praktyce firm i w konwencji dbt prefiksy `dim_`/`fct_`
oznaczają finalne marty gotowe pod Power BI. Powstały dwa problemy:

1. Nazwa sugeruje warstwę konsumpcyjną, której obiekt jeszcze nie reprezentuje.
2. Planowane marty w dbt kolidowałyby nazwami z tabelami źródłowymi (dwa różne
   `dim_customer` w dwóch bazach).

Decyzję podjęto przed zbudowaniem martów, gdy koszt zmiany był jeszcze mały. Jako
drugą opinię zasięgnięto ChatGPT (krytyczna ocena opcji), a plan zmiany przeszedł
przez Plan Mode w Claude Code.

## Decyzja

1. **Opcja A: rename tabel Iceberg** na `customer`, `product`, `orders`,
   `order_items`, `payments`, `order_status_history`. Prefiksy `dim_`/`fct_`
   zarezerwowane wyłącznie dla martów w dbt.
2. **Reguła dla Gold:** obiekt w Gold powstaje tylko wtedy, gdy dodaje logikę
   (joiny, agregacje, atrybuty pochodne). Gold `dim_customer` będący kopią
   `stg_customers` nie powstaje. Marty odpowiadają na pytania biznesowe
   (`fct_customer_revenue`, `fct_product_sales`), a wymiar w Gold ma sens tylko
   wzbogacony (np. `first_order_date`, łączny przychód).
3. Zmieniamy wyłącznie nazwy tabel. Nazwy kolumn, nazwa bazy Glue
   `portfolio_ecommerce` i nazwy modeli `stg_*` zostają. (Ścieżki S3 pierwotnie
   też miały zostać, zmienione 2026-10-07, patrz „Aktualizacja” niżej.)

## Rozważane alternatywy

**B: zostawić nazwy, opisać w dokumentacji.** Odrzucone: dokumentacja jest gorszym
interfejsem niż dobra nazwa, a wyjątek trzeba by tłumaczyć każdemu nowemu człowiekowi.
Koszt mylącej nazwy płaci się przy każdym onboardingu i code review, koszt rename
jednorazowo.

**C: osobne namespace'y dla warstw** (np. Glue database `silver`, `dim_`/`fct_` w
osobnym schemacie Gold). Odrzucone na teraz, nie w zasadzie: nazwa bazy Glue jest
wpisana w policy IAM (ARN-y zasobów), `CATALOG_NAMESPACE` catalog integration,
catalog-linked database, konfigurację Sparka i `source()` w dbt, więc to zmiana
kontraktu w pięciu miejscach zamiast rename sześciu tabel. Separację warstw i tak
daje podział baz po stronie Snowflake (`portfolio_ecommerce_db` vs `portfolio_dbt`).
Do rozważenia przy większej skali projektu.

## Jak wykonano rename (wzorzec migracji)

Glue nie ma natywnego renamu; `GlueCatalog` w Iceberg realizuje go jako utworzenie
nowego wpisu i usunięcie starego, bez ruszania plików. Przez Spark
(`ALTER TABLE ... RENAME TO`, iceberg-spark-runtime 1.9.1): snapshot przed
(liczba wierszy, `metadata_location`), pilotaż na tabeli bez polityk
(`dim_product`), pozostałe tabele pojedynczo z kontrolą po każdej, snapshot po i
porównanie 6/6, dopiero potem aktualizacja kodu, pauza przed ekspozycją w Snowflake.
Zakaz `DROP ... PURGE`. Fallback (`register_table` + usunięcie wpisu bez purge)
niepotrzebny.

## Konsekwencje

**Pozytywne:**
- Nazwa mówi prawdę o warstwie, brak kolizji z martami.
- Marty projektowane od pytań biznesowych, bez powielania modelu wymiarowego.
- Udokumentowany, powtarzalny wzorzec bezpiecznej migracji tabel Iceberg.

**Negatywne / do pamiętania:**
- **Governance jest przypięte do obiektu w katalogu, nie do danych.** Dla Snowflake
  rename w Glue to "stara tabela zniknęła, pojawiła się nowa": polityki masking/RLS
  (i granty per tabela) trzeba założyć ponownie ręcznie i przetestować.
- ~~Katalogi danych w S3 zachowały stare nazwy (`.../dim_customer/`). Świadomie
  przyjęty koszt.~~ Nieaktualne od 2026-10-07, patrz „Aktualizacja”.
- Starsze sekcje `PROGRESS_LOG.md`, ADR-y i ogólne przykłady w cheat sheecie
  zachowują stare nazwy jako zapis stanu z tamtego czasu.

## Aktualizacja 2026-10-07: przeniesienie ścieżek S3

**Zmiana decyzji:** ścieżki S3 zostały dopasowane do nazw tabel
(`.../portfolio_ecommerce.db/customer/` zamiast `.../dim_customer/` itd.).
Rozjazd nazwy w katalogu i katalogu w S3 okazał się realnym kosztem przy
nawigacji po buckecie, a zrobienie tego przed Fazą 6 (Airflow) oznacza, że DAG
od początku działa na ostatecznym układzie.

**Metoda:** `rewrite_table_path` (przepisanie ścieżek w metadanych) + kopia
plików po stronie S3 + `register_table` w osobnej bazie Glue
`portfolio_ecommerce_relocation` (niewidocznej dla Snowflake, bo rola catalog
integration ma uprawnienia tylko do `portfolio_ecommerce`) + zamiana przez dwa
renamy. Odrzucony CTAS: tracił historię snapshotów (w tym punkt rollbacku z
§23) i wymagał ręcznego odtworzenia specyfikacji partycji. Pilot na `product`,
potem pojedynczo, `customer` ostatni.

**Wynik:** 6/6 tabel, wiersze, hash zawartości, snapshoty i time travel
identyczne, zmieniła się tylko lokalizacja. Stare katalogi usunięte po
weryfikacji. Skrypt i dowody: `scripts/repairs/2026-10-07_relocate_table_paths/`.

**Odkrycie (uzupełnia konsekwencję o governance):** przeniesienie z
zachowaniem nazwy + `ALTER ICEBERG TABLE ... REFRESH` w Snowflake **zachowuje**
polityki i granty, bo dla Snowflake to ten sam obiekt z nowym wskaźnikiem
metadanych. Gubi je dopiero zmiana nazwy.

## Powiązane

- ADR-002 (ta sama baza Glue i bucket S3), ADR-004 (catalog integration i
  policy IAM z wildcardem `table/portfolio_ecommerce/*`, który rename nie łamie)
- `PROGRESS_LOG.md`, sekcja 21 (rename) i 25 (przeniesienie ścieżek)
