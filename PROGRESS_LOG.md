# Portfolio Project — Dziennik budowy

Chronologiczny zapis: co zrobiliśmy, w jakiej kolejności, i jakie pytania/
odpowiedzi padły po drodze. Teoria ogólna z przykładami żyje w
`DE_Interview_Cheatsheet.docx` — tu jest historia konkretnie TEGO projektu.

---

## 1. Środowisko lokalne (macOS)

**Zrobione:** Homebrew → Git + konfiguracja SSH do GitHuba → `uv` → Docker Desktop.

**Pytania/odpowiedzi:**
- *Czym jest Homebrew, jakie są alternatywy?* — menedżer pakietów macOS (jak `apt`); alternatywy: MacPorts, Nix, ręczna instalacja, pyenv/nvm (te ostatnie tylko per-język, nie zastępują Homebrew).
- *Jak skonfigurować SSH do GitHuba?* — `ssh-keygen -t ed25519`, dodanie do ssh-agenta/Keychain, wklejenie klucza publicznego w GitHub → Settings → SSH keys, test przez `ssh -T git@github.com`.
- *Dlaczego `ed25519` w nazwie klucza?* — nazwa algorytmu (krzywa eliptyczna Curve25519, liczba 2²⁵⁵−19 to STAŁA matematyczna, publiczna dla wszystkich — unikalny jest dopiero losowy klucz prywatny wygenerowany NA tej krzywej).
- *Jak mieć dwa różne klucze SSH (np. do routera i GitHuba)?* — `-f ~/.ssh/id_ed25519_nazwa` przy generowaniu + osobne bloki `Host` w `~/.ssh/config`.
- *utknięcie w `vim`* — `Esc` + `:q!` (wyjście bez zapisu); polecono `nano` do prostych edycji zamiast `vim`.
- *Dlaczego `uv`, nie `requirements.txt`/`pip`?* — `uv` = szybszy następca `pip`, wbudowany lock file (`uv.lock`), zarządza też wersją Pythona. `pip`/`requirements.txt` bywa nadal używane tam, gdzie dokumentacja/ekosystem (np. Airflow) jeszcze nie nadążyła — `uv pip install` jest w pełni kompatybilne wstecz.
- *Dlaczego Python 3.12, nie najnowszy (3.14/3.15)?* — kompatybilność z PySpark (opóźnienie we wspieraniu nowych wersji), dojrzałość wheels dla pakietów natywnych (psycopg2, pyarrow), "boring technology" jako sygnał na rozmowie.

## 2. Struktura repo i pierwsze pliki

**Zrobione:** `docker-compose.yml` (postgres-source, postgres-airflow, airflow-init/webserver/scheduler), `sql/ddl.sql` (6 tabel), `airflow-docker/Dockerfile`+`requirements.txt`.

**Pytania/odpowiedzi:**
- *Co to `docker-compose.yml`, czy to uniwersalny plik?* — NIE, szyty pod konkretny projekt; Docker pakuje CAŁE środowisko (system, dowolny język), nie tylko zależności Pythona.
- *Błąd YAML: `depends_on` z myślnikiem + zagnieżdżonym `condition`* — dwie różne składnie (`depends_on: [- nazwa]` vs `depends_on: {nazwa: {condition: ...}}`) nie mieszają się; poprawka na formę mapowania.
- *Błąd: komenda `airflow-init` rozjechana na osobne linie bash* — YAML folded scalar (`>`) łączy linie spacją TYLKO przy identycznym wcięciu; głębsze wcięcie zachowuje prawdziwe newline'y. Naprawa: jedna płaska linia.
- *`airflow-init` failuje przy DRUGIM uruchomieniu* — `airflow users create` nie jest idempotentne (user już istnieje); naprawa: `|| true` żeby połknąć ten konkretny, oczekiwany błąd.
- *Dlaczego `SERIAL` zamiast `INT` na PK?* — surrogate key: `INTEGER` + `SEQUENCE` + `DEFAULT nextval(...)` w jednym, auto-increment bez ręcznego zarządzania unikalnością.
- *Co robi `CREATE INDEX` na `updated_at`?* — B-tree przyspieszający `WHERE updated_at > watermark`, koszt: spowalnia zapisy.
- *Co robi `CREATE TRIGGER` + `CREATE OR REPLACE FUNCTION ... RETURNS TRIGGER`?* — dwa osobne obiekty: funkcja (logika) + trigger (podpięcie do zdarzenia `BEFORE UPDATE`). `NEW`/`OLD` to zmienne wstrzykiwane automatycznie przez Postgresa przy wywołaniu jako trigger.
- *Dlaczego `WHERE updated_at > watermark` łapie też NOWE wiersze, nie tylko update'y?* — `DEFAULT now()` działa też przy INSERT, nie tylko trigger przy UPDATE.
- *Czy UPDATE zachowuje starą wersję wiersza?* — NIE (SCD Type 1, nadpisanie w miejscu); historia (SCD2) to świadomie osobna warstwa dalej w pipeline (dbt snapshot/MERGE na Iceberg), nie coś, co OLTP ma robić samo.
- *Czy trzeba ręcznie odpalać VACUUM?* — nie, `autovacuum` domyślnie włączony, sprząta MVCC dead tuples w tle automatycznie.

## 3. AWS setup od zera

**Zrobione:** konto AWS (root) → budget alert → IAM user (`portfolio-pipeline`, bez dostępu do konsoli) → policy `AmazonS3FullAccess` → access key (use case "Local code") → `awscli` przez Homebrew → `aws configure` (region `eu-central-1`) → bucket S3 (`tp-portfolio-ecommerce-raw`).

**Pytania/odpowiedzi:**
- *Dlaczego "access key", nie "API key"/SSH?* — nazewnictwo specyficzne dla AWS; SSH to inny protokół (logowanie do maszyny), nieużywany do wywołań API.
- *Czy do AWS loguje się przez Gmail?* — tak, root user = zwykły mail + hasło; IAM user loguje się inaczej (ID konta + username), ale u nas świadomie bez dostępu do konsoli w ogóle.
- *Czy user1/portfolio-pipeline to jak nowy pracownik w korporacji?* — częściowo; w dojrzałych firmach ludzie logują się przez SSO/IAM Identity Center (federacja), pojedynczy IAM user z access key zostaje jako wzorzec dla **service accountów** (automatyzacji), nie ludzi.
- *Trafiłem przypadkiem na "IAM Identity Center" (multi-region, KMS charges)* — to inna usługa niż zwykłe IAM; Cancel, szukać czystego "IAM" bez "Identity Center".
- *`uv install awscli`?* — NIE; `awscli` to systemowe narzędzie (Homebrew), nie biblioteka Pythona importowana przez kod — `uv` zarządza tylko zależnościami projektu.
- *Dlaczego bucket w `us-east-1` a `aws configure` ma `eu-central-1`?* — okazało się że to był tylko domyślny widok UI przed przeładowaniem; `aws s3api get-bucket-location` potwierdził zgodność `eu-central-1`.

## 4. Seed danych (`seed.py`)

**Zrobione:** Faker (`Faker.seed(42)`) → 50 klientów, 30 produktów, 200 zamówień + item/payment/status history, przetestowane end-to-end (osobny Postgres w sandboxie), potwierdzone identyczne liczby u Ciebie (56/47/14/40/43 status breakdown).

**Pytania/odpowiedzi:**
- *Co to `psycopg2`?* — driver/adapter Pythona do Postgresa, `psycopg2-binary` = prekompilowane binarki.
- *Dlaczego `uv run python skrypt.py`, nie `python skrypt.py`?* — `.venv` to izolowane środowisko z faktycznie zainstalowanymi zależnościami; gołe `python` użyje systemowego Pythona bez tych pakietów.
- *Co to lock file, ogólnie?* — plik generowany automatycznie, dokładne wersje CAŁEGO drzewa zależności; ten sam koncept co `package-lock.json` (npm), `Gemfile.lock` (Ruby), `Cargo.lock` (Rust).
- *Skoro mam `uv.lock`, to `pyproject.toml` jest zbędny?* — NIE; `pyproject.toml` = intencja (edytowana przez `uv add`/`uv remove`), `uv.lock` = dokładny, automatycznie wyliczony wynik tej intencji. Oba commitowane razem.

## 5. Incremental extract: Postgres → S3 (`extract.py`)

**Zrobione:** watermark per tabela w lokalnym pliku JSON, `MAX(watermark)` z faktycznie pobranych wierszy jako nowy punkt odcięcia, append-only partycjonowanie w S3 (`ingestion_date=.../run_id=...`). Przetestowane: pierwszy przebieg (pełny), drugi przebieg (zero nowych), UPDATE złapany poprawnie (dokładnie 1 wiersz).

**Pytania/odpowiedzi:**
- *Co robi `load_watermark_state()`, czemu `return {}` bez `else`?* — "early return": `return` w pierwszej gałęzi kończy funkcję, więc kod niżej wykonuje się TYLKO gdy ta gałąź nie zaszła — funkcjonalnie identyczne do jawnego `else`.
- *Co robi `try/finally`?* — gwarantuje wykonanie sprzątania (`connection.close()`) niezależnie czy w bloku `try` wystąpił błąd, czy nie — bez tego błąd w połowie pętli zostawiłby otwarte połączenie.
- *Dlaczego watermark łapie nowe ORAZ zmienione rekordy, nie tylko "nowe"?* — bo pyta "co jest nowsze niż ostatni checkpoint", nie "czy było update'owane" — insert i update oba ustawiają `updated_at`.
- *Jak wygląda to na produkcji (nie lokalny plik)?* — dwa wzorce: (A) zewnętrzny store stanu (Airflow Variable/tabela kontrolna), (B) zapytanie `MAX(watermark)` bezpośrednio do tabeli DOCELOWEJ (Snowflake/Iceberg) — samo-naprawiające się, brak osobnego stanu do zgubienia. Lokalny JSON to świadomy placeholder do czasu istnienia tabeli docelowej.

## 6. Repo na GitHubie

**Zrobione:** `.gitignore` (świeżo dopisany, bo `uv init` go nie stworzyło — folder już był repo gita), pierwszy commit, nowe puste repo na GitHubie (Public, bez README/gitignore z ich strony), `git remote add origin` (SSH), `git push -u origin main`.

**Pytania/odpowiedzi:**
- *Dlaczego `.gitignore` nie istniał mimo wcześniejszego `uv init`?* — `uv init` tworzy `.gitignore` tylko gdy SAMO zakłada nowe repo gita; jeśli folder już był repo (`.git` istniał), pomija ten krok.
- *Co robią `git remote add origin`/`git branch -M main`/`git push -u origin main`?* — kolejno: zarejestrowanie adresu zdalnego repo pod aliasem `origin`; wymuszenie nazwy gałęzi `main`; faktyczny transfer + zapamiętanie powiązania lokalna↔zdalna gałąź (`-u`).
- *Jak działa port/IP (SSH na porcie 22)?* — IP = adres budynku, port = numer pokoju w środku; port 22 to konwencja IANA (rejestr), nie fizyczna właściwość — serwer SSH może nasłuchiwać na dowolnym innym porcie.
- *Czym różni się SSH (do `git`) od PAT+HTTPS (do REST API)?* — SSH = protokół transportowy używany przez `git` do push/pull; PAT = poświadczenie wysyłane WEWNĄTRZ zwykłych zapytań HTTPS do REST API — dwie niezależne warstwy (transport vs. tożsamość), nie warianty tego samego.

## 7. GitHub API extract (`github_extract.py`)

**Zrobione:** Fine-grained PAT ("Public Repositories, read-only"), `.env` + `python-dotenv`, 4 encje (`repositories`/`issues`/`pull_requests`/`contributors`) z `apache/airflow`, trzy różne wzorce incremental per endpoint (GitHub API narzuca różne ograniczenia), paginacja przez `Link` header, multithreading przez `ThreadPoolExecutor`+`as_completed`.

**Znalezione i naprawione bugi (realne, empiryczne):**
- GitHub API: `since=1970-01-01` (epoka) zwraca CICHO pustą listę (status 200, `[]`) — niedokumentowany edge case; naprawa: pomiń `since` całkowicie przy pierwszym przebiegu zamiast epoki.
- Pierwszy przebieg `pull_requests` wisiał minutami — sekwencyjna paginacja przez CAŁĄ historię PR-ów bardzo aktywnego repo; naprawa: ograniczenie pierwszego przebiegu do 90 dni wstecz + twardy limit 20 stron jako zabezpieczenie + logowanie postępu strona po stronie.

**Pytania/odpowiedzi:**
- *Co to PAT, jakie są alternatywy?* — token identyfikujący usera wobec API (zamiast login+hasło, wyłączone przez GitHub w 2021); alternatywy: OAuth App (loguje wielu userów), GitHub App (integracja na poziomie organizacji), classic PAT (szersze zakresy, mniej precyzyjny).
- *Jakie dane realnie ciągniemy z `apache/airflow`?* — publiczne metadane repo/issues/PR-ów/kontrybutorów, dokładnie to co widać na stronie GitHuba, tylko jako JSON zamiast HTML.
- *Co robi `as_completed`?* — NIE dodaje współbieżności (to robi `submit()`); zmienia tylko KOLEJNOŚĆ odbioru wyników na "w miarę kończenia" zamiast "w kolejności zlecenia".
- *`multiprocessing.pool.ThreadPool` vs `concurrent.futures.ThreadPoolExecutor`?* — dwa niezależne dialekty tego samego pomysłu (starszy vs nowszy), różnią się tylko stylem API (`.map()` vs `.submit()`+`as_completed`), nie mają związku z tym, czy kod dotyczy Sparka czy nie.
- *Kiedy wątki faktycznie pomagają?* — I/O-bound (czekanie na sieć/dysk/klaster) — GIL zwolniony podczas czekania. CPU-bound (liczenie w pamięci) — wątki NIE pomagają przez GIL, trzeba `ProcessPoolExecutor`.
- *Czym są wątki, intuicyjnie?* — proces = budynek (odizolowana pamięć), wątek = pracownik wewnątrz (dzieli pamięć z innymi wątkami); GIL = "jedna ksero-maszyna na budynek".
- *Kod ze Sparkiem z `ThreadPool` — czy to ma sens, gdzie tu wchodzi Spark?* — TAK, to legit i udokumentowana technika (Spark "Scheduling Within an Application"); trzy warstwy: (A) Spark sam rozdziela JEDNO zapytanie na executory (JVM, niezależne od Pythona), (B) wątki na driverze decydują ile NIEZALEŻNYCH zapytań do Sparka lecą naraz (czysty Python, ten sam GIL/I-O co przy API), (C) Fair Scheduler Sparka (opcjonalny, trzeba włączyć `spark.scheduler.mode=FAIR`) sprawiedliwie dzieli zasoby klastra między równoległe joby z wątków.
- *Czy Fair Scheduler trzeba zawsze dodawać?* — NIE, domyślny FIFO w pełni poprawny funkcjonalnie; FAIR to opcjonalna optymalizacja przy wielu jobach różnej wielkości/ograniczonych zasobach klastra.
- *Znaleziony bug w przykładowym kodzie usera (spoza tego projektu)* — gałąź `else` (`multithread_processing=False`) miała `.append()` POZA pętlą (złe wcięcie) — działałoby tylko dla ostatniej iteracji; plus brakująca inicjalizacja `schema_output=[]` w tej gałęzi.

## 8. Walidacja danych (`validate.py`, Faza 2)

**Zrobione:** per-tabela `TABLE_EXPECTATIONS` (required_columns, not_null, PK, accepted_values), znajdowanie najnowszego pliku w S3 po `LastModified`, walidacja + kwarantanna (nic nie ginie, wszystko odrzucone trafia do `quarantine/` z dopisanym powodem). Przetestowane na zamockowanych złych danych (null, duplikat PK, zły status, brakujące kolumny) — wszystkie 4 mechanizmy złapane poprawnie. Na prawdziwych danych: 0 quarantined (dane czyste).

**Pytania/odpowiedzi:**
- *Czy nadmiarowe kolumny wywalają błąd?* — nie, sprawdzane są tylko BRAKUJĄCE wymagane kolumny, nadmiarowe przechodzą bez ruszenia.
- *Czy `validate_row` mówi KTÓRY wiersz ma błąd?* — pośrednio tak: cały oryginalny wiersz + `_quarantine_reasons` trafia razem do pliku w S3, więc kontekst (PK i inne pola) jest zawsze widoczny obok powodu.
- *Set vs lista przy sprawdzaniu PK uniqueness — czy set to overkill?* — NIE; `set` (hash table) daje sprawdzenie przynależności ~natychmiastowe niezależnie od rozmiaru, lista wymaga przejrzenia wszystkiego po kolei (O(n²) łącznie dla listy vs O(n) dla seta przy dużej skali).
- *Dlaczego PK uniqueness liczone PRZED główną pętlą, nie w jej trakcie?* — duplikat to własność CAŁEGO batcha, nie pojedynczego wiersza; nie da się ocenić "czy to duplikat" patrząc tylko na to, co już widziałeś w pętli — trzeba znać cały zbiór z góry.
- *Co robi `_` na początku `_quarantine_reasons`?* — czysta konwencja nazewnicza (nie wymóg języka), sygnalizuje "to metadana dołożona przeze mnie, nie oryginalna kolumna źródłowa".
- *Co robi `{value!r}` we f-stringu?* — `!r` = użyj `repr()` zamiast `str()` — pokazuje "programistyczną" reprezentację (np. cudzysłowy wokół stringów), pomaga odróżnić string od innych typów w komunikatach diagnostycznych.
- *Set vs tuple?* — tuple: uporządkowana, niezmienna, duplikaty dozwolone (do grupowania stałych wartości); set: nieuporządkowany, bez duplikatów, zoptymalizowany pod szybkie sprawdzanie przynależności — różne przeznaczenie, nie warianty tego samego.
- *Jaka jest procedura obsługi kwarantanny?* — na tym etapie w pełni RĘCZNA (ręczny przegląd `s3://.../quarantine/`); automatyzacja alertów/reakcji to zakres Advanced (pełny Great Expectations), świadomie odłożony.

## 9. PySpark + Iceberg (Faza 3, w toku)

**Zrobione:** `spark-docker/` (nowy serwis `spark-iceberg` w `docker-compose.yml`, stoi w tle jako "workbench" — `docker exec` żeby odpalać skrypty), IAM user rozszerzony o `AWSGlueConsoleFullAccess`, baza `portfolio_ecommerce` w Glue Data Catalog, `merge_customers.py` (pełne SCD2 przez `MERGE INTO` w dwóch krokach na tabeli Iceberg).

**Napotkane i naprawione problemy (realne, empiryczne):**
- `bitnami/spark:3.5` — Bitnami/Broadcom w 2025 usunęło darmowe tagi wersji z Docker Hub ("No tags available"). `apache/spark-py` (oficjalny) nieaktualizowany od ~3 lat, max `v3.4.0` (za stary względem jarów Iceberga dla 3.5). Naprawa: własny `Dockerfile` od `eclipse-temurin:17-jdk-jammy` (oficjalny, aktywnie utrzymywany OpenJDK) + `pip install pyspark==3.5.5` — pełna kontrola nad wersją, mniej zależności od cudzych decyzji biznesowych.
- `SdkClientException: Unable to load region` przy próbie `CREATE TABLE` przez Glue — AWS SDK v2 dla Javy (wewnątrz `iceberg-aws-bundle`, używane przez Iceberg/Spark) czyta WYŁĄCZNIE `AWS_REGION`, nie `AWS_DEFAULT_REGION` (którego używa `boto3`/Python). Naprawa: ustawienie OBU zmiennych w `docker-compose.yml`.

**Pytania/odpowiedzi:**
- *Dlaczego Docker zamiast instalować Sparka/Javę lokalnie na macOS?* — PySpark wymaga JVM niezależnie od `pip install`; `import pyspark` się uda, ale realne użycie (`SparkSession`) wywali błąd bez Javy. Docker unika zmagań z wersjami Javy na hoście, spójnie z resztą projektu.
- *Co się zmieniło, że trzeba było zmienić podejście (Bitnami)?* — zewnętrzna zmiana polityki dostawcy obrazu, nie błąd w projekcie; dobra ilustracja ryzyka polegania na cudzym, "wygodnym" obrazie bazowym (analogicznie do vendor lock-in, cheat sheet 15.4).
- *Czy `spark-docker/Dockerfile` to instrukcja pobrania binarek?* — tak: `FROM` = baza (Docker Hub), `apt-get install` = pakiety Ubuntu, `pip install pyspark` = Spark+Python z PyPI, `curl` = jary Iceberga z Maven Central — cztery różne źródła, każde pobierane i składane w jeden gotowy obraz.
- *Dlaczego `docker exec -it spark-iceberg ...` i "dziwna" ścieżka `/opt/spark-scripts/...`?* — `spark-iceberg` to nazwa kontenera (jak wcześniej `postgres-source`); ścieżka to adres WEWNĄTRZ kontenera, inny niż na hoście, bo to inny, odizolowany system plików.
- *Dlaczego potrzebujemy pliku pod dwoma adresami, czemu kontener nie widzi folderu hosta normalnie?* — kontenery są DOMYŚLNIE odizolowane od hosta (cecha bezpieczeństwa, nie ograniczenie); bind mount (`volumes: - ./scripts:/opt/spark-scripts`) to jawny, świadomy wyjątek — TEN SAM plik fizyczny, dostępny pod dwoma ścieżkami, nie kopia (kontrast z `COPY` w Dockerfile, które robi prawdziwą kopię raz, podczas builda).
- *Czy przechodzimy z boto3 na AWS SDK v2 dla Javy?* — NIE; oba współistnieją w jednym kontenerze, po dwóch stronach mostu py4j (sekcja 22.6) — boto3 po stronie Pythona (ściąganie surowych plików z S3), AWS SDK v2 dla Javy wewnątrz JVM/Iceberga (operacje na metadanych tabeli przez Glue), niezależnie od siebie.
- *Co robią `up -d`/`--build`?* — `-d` = w tle; `--build` = wymuś przebudowanie obrazu (potrzebne przy zmianie Dockerfile, niepotrzebne przy samej zmianie zmiennych środowiskowych w docker-compose.yml).

*Status: skrypt `merge_customers.py` w trakcie debugowania na żywo — kolejne przebiegi/poprawki dopiszę tutaj.*

**Kolejny znaleziony i naprawiony bug:** `changed_customer_ids` jako `TEMP VIEW` (leniwie liczony) zamiast konkretnej listy — przy odwołaniu się do niego w Kroku 2, Spark PRZELICZAŁ zapytanie na nowo, na stanie tabeli PO już wykonanym MERGE (gdzie zmieniony wiersz miał już `is_current=false`), więc wynik był pusty. Naprawa: `.collect()` do zwykłej listy Pythona OD RAZU po policzeniu, PRZED uruchomieniem MERGE — "zamraża" wynik, zanim tabela się zmieni.

**Zweryfikowane pełne SCD2 na żywo:** `customer_id=5`, zmiana `country` LY→FR w Postgresie → `extract.py` złapał deltę → `merge_customers.py` poprawnie zamknął starą wersję (`is_current=false`, `valid_to` ustawione) i otworzył nową (`is_current=true`, `valid_to=NULL`) — dokładnie "wow moment" z dokumentu projektowego.

**AWS Athena postawiona jako alternatywny, lżejszy sposób odczytu tabel Iceberg** — bez Sparka/kontenera, czysty SQL przez Glue Data Catalog. Potwierdzone: to samo zapytanie dające identyczny wynik przez Athenę (566 ms) i przez Sparka — dowód, że odczyt nie wymaga Sparka (tylko zapis/MERGE go wymaga), zgodnie z uzasadnieniem wyboru Iceberga (multi-engine, cheat sheet 15.1).

**Pytania/odpowiedzi (dalszy ciąg):**
- *Czy ta komenda testowa łączy się z S3?* — tak, z DWOMA usługami: Glue (metadane/schemat/lokalizacja) + S3 (rzeczywiste pliki danych tabeli).
- *Czy dało się to sprawdzić prościej, bez budowania Sparka?* — tak, do ODCZYTU wystarczy silnik czytający Glue Catalog (Athena, Trino, docelowo Snowflake); Spark jest konieczny tylko do ZAPISU/MERGE, bo tylko on fizycznie przepisuje pliki tabeli.
- *Athena z terminala przez SDK?* — tak (`aws athena start-query-execution` / `get-query-execution` / `get-query-results`, asynchronicznie w 3 kroki — Athena to usługa serverless w tle, nie lokalny proces jak `psql`), wymaga: polityki `AmazonAthenaFullAccess`, lokalizacji wyników w S3 (`athena-results/`), jednorazowej konfiguracji workgroup.
- *Czy AWS SDK v2 dla Javy to coś, co sami wybieramy/instalujemy zamiast boto3?* — nie, przyszło "w pakiecie" wewnątrz `iceberg-aws-bundle`; współistnieje z boto3 w tym samym kontenerze, po dwóch stronach mostu py4j, każde do innych zadań (boto3 — proste operacje na plikach S3 z Pythona; SDK v2 Javy — wewnętrzne operacje Iceberga na Glue/S3).
- *Co to JAR?* — spakowana, skompilowana biblioteka Javy — dokładny odpowiednik paczki Pythona (`pip`/`uv add`), tylko z Maven Central zamiast PyPI, ręcznie pobierana przez `curl` w Dockerfile (Spark nie ma wygodnego "menedżera pakietów" jak `uv`).
- *Czy AWS SDK v2 faktycznie "rozmawia" z AWS?* — tak, to warstwa fizycznie wysyłająca podpisane zapytania HTTP do AWS; Iceberg deleguje do niej techniczną robotę komunikacji sieciowej, sam skupiając się na logice tabeli/MERGE.

## 10. Faza 3 — dokończenie: partycjonowanie, Athena, konsolidacja skryptów

**Zrobione:** partycjonowanie `dim_customer` dodane RETROAKTYWNIE (`ALTER TABLE ... ADD PARTITION FIELD days(valid_from)` — partition evolution, bez przepisywania starych danych) → `merge_products.py` (SCD2, analogiczne do customers) → `merge_facts.py` (generyczny, config-driven, upsert dla `orders`/`order_items`/`payments`, append-only dla `order_status_history`) → na prośbę o unifikację: `merge_customers.py` + `merge_products.py` połączone w jeden `merge_dimensions.py` (config-driven SCD2), stare pliki usunięte → AWS Athena postawiona jako lekka alternatywa do odczytu (bez Sparka) → znaleziony i naprawiony bug: Spark's JSON reader zwraca `STRING` dla pól czasowych (JSON nie ma natywnego typu timestamp) → `MERGE`/`INSERT` do kolumn `TIMESTAMP` wymaga jawnego `F.to_timestamp()` przed użyciem.

**Zasada podziału skryptów (odpowiedź na "czy nie dało się to ujednolicić"):** parametryzuj to, co różni się TYLKO wartościami (nazwy kolumn, klucze) — `merge_dimensions.py` łączy customers+products, bo mają IDENTYCZNY algorytm SCD2. NIE łącz rzeczy różniących się LOGIKĄ (SCD2 dwuetapowe vs prosty upsert jednoetapowy) — `merge_facts.py` zostaje osobno, bo to inny algorytm, nie inna konfiguracja.

**Pytania/odpowiedzi:**
- *Dlaczego Hive nie potrafi partycjonować retroaktywnie, a Iceberg tak?* — w Hive partycja JEST fizyczną ścieżką folderu (dane muszą leżeć w `kolumna=wartość/`), zmiana wymaga przepisania wszystkich plików. W Icebergu partycja to wpis w WARSTWIE METADANYCH (manifest), niezależny od fizycznej lokalizacji pliku — `ALTER TABLE ADD PARTITION FIELD` zmienia regułę na przyszłość, stare pliki zostają nietknięte.
- *Czy Iceberg jest więc "płaski"?* — nie do końca; nadal ma jakąś fizyczną strukturę na dysku, ale silnik NIE odczytuje partycji z nazwy folderu, tylko z metadanych — to metadane są źródłem prawdy, nie ścieżka.
- *Różnica CDC vs SCD?* — CDC (watermark w `extract.py`) to mechanizm TECHNICZNY wykrywania/transportu zmiany ze źródła; SCD (Type 1/2/3, `merge_dimensions.py`) to wzorzec MODELOWANIA, jak zapisać tę zmianę w warstwie docelowej. CDC stosuje się do WSZYSTKICH tabel źródłowych; SCD2 tylko do wymiarów (fakty dostają prosty upsert).
- *Czy zgubię pośrednią zmianę (np. kraj zmieniony 2x podczas przerwy w ekstrakcji) przy watermarku?* — TAK, zawsze, niezależnie od tego czy backfill robi się dzień-po-dniu czy jednym przebiegiem — bo Postgres (OLTP, SCD Type 1) nadpisuje w miejscu, pośredni stan fizycznie znika ze źródła ZANIM pipeline go zobaczy. To ograniczenie watermark-based CDC, nie backfillu — naprawia to tylko log-based CDC (Debezium).
- *Po co więc backfill dzień-po-dniu, skoro ciągły watermark i tak łapie wszystko jednym przebiegiem?* — dla PIPELINE'ÓW SPARAMETRYZOWANYCH DATĄ (np. `WHERE order_date = '2026-09-15'` jako filtr, nie ciągły watermark) dzień-po-dniu jest KONIECZNY, bo każdy dzień to inny wycinek zapytania. Dla ciągłego watermarku (nasz `extract.py`) różnica to głównie izolacja błędu/audytowalność w UI, NIE różnica w złapanych danych.
- *Co to "data logiczna" (`execution_date`/`logical_date`)?* — data ZA JAKI okres dane mają być, niezależna od tego KIEDY fizycznie kod się wykonał (ważne przy backfillu — 25 września odpalony kod może "udawać" że jest 15 września).
- *Dlaczego `valid_from`/`valid_to` powinny brać się z `source.updated_at`, nie `current_timestamp()`?* — event time (kiedy zmiana NAPRAWDĘ się wydarzyła w Postgresie) vs processing time (kiedy skrypt MERGE się odpalił) — jeśli MERGE działa z opóźnieniem względem ekstrakcji (albo przy przebudowie tabeli od zera), `current_timestamp()` dałoby błędne, mylące daty w historii SCD2. NIE naprawia to utraty danych u źródła (inny, głębszy problem) — tylko poprawność dat DLA DANYCH, które faktycznie przetrwały do S3.
- *Czy warto trzymać config (`DIMENSIONS`/`TABLES` dict) w osobnym pliku JSON/YAML zamiast w Pythonie?* — komercyjnie tak przy DUŻEJ skali (metadata-driven pattern, jak własne doświadczenie z P&G/STTM) — YAML/tabela kontrolna pozwala nieprogramistom edytować config, czytelniejszy diff. Przy małej skali (2-4 tabele, jak tu) Python jest czytelniejszy i bezpieczniejszy typowo (type hints) — decyzja o skali, nie uniwersalna zasada.
- *Co robi kontener `spark-iceberg` — czy to tylko komunikacja z AWS SDK?* — NIE, to przede wszystkim SILNIK PRZETWARZANIA (Spark/JVM wykonujący `MERGE`, dedup, itd.); AWS SDK v2 to tylko JEDEN z komponentów wewnątrz, używany gdy Iceberg akurat musi dotrzeć do Glue/S3 — analogia: fabryka (produkuje), nie kurier (tylko dostarcza).
- *Czy taki setup (Spark w jednym kontenerze Docker) używany jest komercyjnie?* — tak, ale WYŁĄCZNIE do local dev/CI/CD/małych zadań — jeden kontener = jedna maszyna, brak prawdziwego rozproszenia. Produkcyjnie: Databricks, AWS EMR/EMR Serverless, AWS Glue Jobs (ten sam Glue już używany jako katalog!), Spark na Kubernetes.
- *Co to sandbox, jak ma się do Dockera?* — sandbox to OGÓLNY KONCEPT (odizolowane środowisko do bezpiecznego testowania), Docker to JEDNA z technologii do jego realizacji (inne: osobne konto AWS, VM, sandbox przeglądarki/JS, środowisko CI). Nie każdy kontener Dockera pełni rolę sandboxa (np. `postgres-source` to realna infrastruktura, nie sandbox) — zależy od CELU użycia, nie samej technologii.

**Kolejny znaleziony i naprawiony bug (ten sam mechanizm co timestamp, inny typ):** `extract.py`'s `json.dumps(..., default=str)` zamienia na string RÓWNIEŻ wartości `Decimal` (nie tylko daty) — `price`/`unit_price`/`amount` też wymagały jawnego rzutowania (`cast(col_type)`) przed zapisem do kolumn `DECIMAL(10,2)` w Icebergu.

**Faza 3 zakończona, w pełni zweryfikowana na prawdziwych danych:** wszystkie 6 tabel poprawnie zmergowane — `dim_customer` (51 wierszy z historią), `dim_product` (30), `fact_orders` (200), `fact_order_items` (502), `fact_payments` (160), `fact_order_status_history` (493) — liczby zgodne z surowymi danymi z `seed.py`.

**Weryfikacja partycjonowania (Athena, `SELECT * FROM "table$partitions"`):** `fact_orders` → 80 partycji (jedna per dzień z aktywnością, rozrzucone 20 czerwca - 13 września, zgodnie z 90-dniowym rozrzutem `seed.py`); `dim_product` → 24 partycje (podobnie rozrzucone, bo nigdy ręcznie nie testowaliśmy UPDATE na products); `dim_customer` → tylko 1 partycja (bo wszystkie SCD2-testowe zmiany działy się "dzisiaj", podczas naszych testów) — wszystkie trzy wyniki poprawne i zgodne z oczekiwaniami, nie błędy.

**Pytania/odpowiedzi (partycjonowanie w praktyce):**
- *Jak zobaczyć partycjonowanie namacalnie (S3/Glue/Athena)?* — `aws s3 ls .../data/ --recursive` pokazuje SUROWE pliki (ale BEZ czytelnych nazw folderów partycji — patrz niżej); najlepszy dowód to Athena `SELECT * FROM "tabela$partitions"` albo Spark `SELECT * FROM catalog.db.tabela.partitions`.
- *Dlaczego pliki w S3 nie mają w nazwie/ścieżce wartości partycji (np. `valid_from_day=...`)?* — skorygowana wcześniejsza odpowiedź: ten silnik zapisu NIE koduje wartości partycji w ścieżce pliku wcale — to jeszcze bardziej "ukryte" partycjonowanie niż opisane pierwotnie; cała informacja żyje WYŁĄCZNIE w metadanych (manifesty Avro), nieczytelnych przez zwykłe `ls`.
- *Gdzie w kodzie widać różnicę upsert (facts) vs append-only (order_status_history)?* — `merge_facts.py`, flaga `config["append_only"]` (per tabela) rozgałęzia wygenerowany SQL: `True` → tylko `WHEN NOT MATCHED THEN INSERT` (brak gałęzi UPDATE w ogóle); `False` → dodatkowo `WHEN MATCHED THEN UPDATE SET ...` (pełny upsert).

## 11. Faza 3 zamknięta — handoff do nowej rozmowy

**Status:** Faza 3 (PySpark + Iceberg) w pełni zakończona i zweryfikowana: MERGE/SCD2/idempotencja/partycjonowanie/Glue REST catalog — wszystkie punkty z ADR-002 pokryte, na prawdziwych danych, z dowodami z dwóch niezależnych silników (Spark do zapisu, Athena do odczytu).

**Decyzja:** kontynuacja (Faza 4 — Snowflake) w NOWYM czacie, żeby uniknąć degradacji jakości w bardzo długiej rozmowie. Ten plik (`PROGRESS_LOG.md`) + `DE_Interview_Cheatsheet.docx` to punkt wejścia dla nowego czatu — muszą być wgrane jako aktualne pliki projektu (nie tylko pobrane lokalnie), żeby nowa rozmowa miała pełny kontekst bez powtarzania historii.

**Co powinien wiedzieć nowy czat na start:** cała infrastruktura stoi (`docker-compose.yml`: Postgres źródłowe+metadanych, Airflow, Spark+Iceberg), AWS gotowe (S3 bucket, IAM user `portfolio-pipeline`, Glue Data Catalog z bazą `portfolio_ecommerce`, Athena skonfigurowana), 6 tabel Iceberg istnieje i ma dane (`dim_customer`, `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, `fact_order_status_history`), GitHub ingestion też gotowe (4 encje z `apache/airflow`). Następny krok wg planu: Faza 4 — integracja Snowflake z tymi samymi tabelami Iceberg przez Glue Catalog, RBAC, dynamic masking, RLS.

## 12. Faza 4 — start: plan i decyzje integracji Snowflake

**Kontekst wejściowy (nowy czat):** cała infrastruktura z Fazy 1-3 stoi i działa —
AWS (S3 bucket, IAM user `portfolio-pipeline`, Glue Data Catalog z bazą
`portfolio_ecommerce`, Athena), Docker (Postgres źródłowe+metadanych, Airflow,
Spark+Iceberg), 6 tabel Iceberg z realnymi danymi.

**Plan Fazy 4 (ustalony):**
1. Snowflake trial (30 dni, $400 kredytu), AWS + `eu-central-1`
2. IAM rola + trust policy (external ID) dla Snowflake
3. Catalog integration typu REST (`ICEBERG_REST`, `CATALOG_API_TYPE = AWS_GLUE`, `SIGV4`)
4. External volume → S3 bucket
5. Catalog-linked database (auto-discovery istniejących 6 tabel z Glue, bez ręcznych `CREATE ICEBERG TABLE`)
6. RBAC (hierarchia ról, granty)
7. Dynamic data masking (np. `email` w `dim_customer`)
8. Row-level security (np. widoczność zamówień per region/segment)
9. Weryfikacja przez Query Profile (dowód odczytu z Iceberga/S3, nie kopii)

**Decyzja (pełne uzasadnienie i odrzucone alternatywy → `ADR-004`):**
- REST catalog integration, nie klasyczna `CATALOG_SOURCE = GLUE` (Snowflake
  jawnie rekomenduje REST, klasyczna ścieżka oznaczona jako do wygaszenia).
- `ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS`, nie `VENDED_CREDENTIALS`
  — unikamy wprowadzania AWS Lake Formation jako nowej warstwy dostępu obok już
  działającego modelu IAM-role-per-service; ten sam wzorzec decyzyjny co przy
  Python dict vs YAML w Fazie 3 (nie dodawaj złożoności bez wymuszenia skalą).
- Catalog-linked database zamiast ręcznej deklaracji tabel — pasuje do tego,
  że tabele już istnieją z Fazy 3, nie są tworzone od zera przez Snowflake.

**Status:** plan zaakceptowany, start od punktu 1 (założenie konta Snowflake trial)
w kolejnej wiadomości.

## 13. Faza 4 — konto Snowflake założone, primer podstaw

**Zrobione:** rejestracja trial (30 dni, $400 kredytu) — edycja **Enterprise**
(wymagana dla Dynamic Data Masking i Row Access Policies z punktów 7-8 planu;
Standard ich w ogóle nie ma — twardy gate na poziomie edycji, nie ustawienie do
włączenia), cloud **AWS**, region **EU (Frankfurt)** — zgodność z S3/Glue.
Zalogowano na roli `ACCOUNTADMIN` (super-admin, używany tylko do setupu, nie
do codziennej pracy — część tematu RBAC z punktu 6).

**Pytania/odpowiedzi:**
- *Czy trial jest darmowy, edycja Enterprise vs Standard coś kosztuje w trialu?*
  — w pełni darmowe, brak karty kredytowej; obie edycje kosztują dokładnie tyle
  samo z puli $400 podczas trialu, różnica w cenie kredytu (Standard $2 vs
  Enterprise $3) liczy się dopiero PO trialu, przy przejściu na płatne konto.
  Realny koszt do pilnowania to zużycie kredytu (storage + compute), nie edycja
  — auto-suspend na warehousie ma zostać włączony, nie wyłączany.
- *Warto dołączyć Snowflake CoCo (AI coding agent natywny dla Snowflake) do
  projektu?* — odrzucone. Sprzeczne z ADR-003 (świadomie ograniczony workflow
  Claude Code + Codex); ryzyko, że AI napisze RBAC/masking/RLS za użytkownika,
  co podważa cel Fazy 4 (rozumienie tych mechanizmów pod interview). Ewentualne
  wspomnienie jako "awareness" w cheat sheecie odłożone, niezrobione.
- *Czy dane na Snowflake to tylko metadane?* — nie: metadane (schema, manifesty
  Iceberga, statystyki do file pruning) są jedyną rzeczą synchronizowaną przez
  Snowflake; same wartości wierszy fizycznie zostają w plikach Parquet w S3 i są
  czytane na żywo przy każdym zapytaniu przez compute Snowflake (warehouse) —
  ten sam wzorzec multi-engine co Spark/Athena w Fazie 3, nie kopia danych do
  storage'u Snowflake (co różni to od zwykłej natywnej tabeli Snowflake).
- *Co to manifest, Avro vs Parquet vs Delta transaction log?* — pełne
  rozwinięcie z przykładami i analogią do `git checkout` vs kolejki łatek
  przeniesione do cheat sheeta, sekcja **15.1.1** (nowa), żeby nie duplikować
  ogólnej teorii w dwóch miejscach — tu tylko odnotowanie, że temat był i gdzie
  szukać szczegółów.

**Primer podstaw Snowflake (przed IAM/catalog integration):** warehouse (silnik
compute, nie storage — od tego płacisz), database → schema → table (analogicznie
do `katalog.schemat.tabela` w Glue/Sparku), RBAC przez role (nie bezpośrednio na
usera), worksheet (SQL przez UI, odpowiednik `docker exec` do Sparka),
`ACCOUNTADMIN` jako rola startowa tylko do setupu, model kosztów (storage tanie/
prawie zerowe u nas, bo dane leżą w S3; compute = warehouse, płatne tylko gdy
"chodzi").

**Status:** gotowość do kroku 2 planu — IAM rola + trust policy dla Snowflake.

## 14. Faza 4 — catalog integration domknięta (Glue REST)

**Zrobione:** IAM policy `snowflake-glue-catalog-read` (read-only: `GetCatalog`/
`GetDatabase`/`GetDatabases`/`GetTable`/`GetTables`, scoped do bazy
`portfolio_ecommerce`) → IAM rola `snowflake-glue-catalog-role` z tymczasowym
trust policy (placeholder: zaufanie do własnego konta + dummy external ID) →
`CREATE CATALOG INTEGRATION glue_rest_catalog_int` w Snowflake (`ICEBERG_REST`,
`CATALOG_API_TYPE = AWS_GLUE`, `SIGV4`, `ACCESS_DELEGATION_MODE =
EXTERNAL_VOLUME_CREDENTIALS` zgodnie z ADR-004) → `DESCRIBE CATALOG INTEGRATION`
→ odczytane `GLUE_AWS_IAM_USER_ARN` + `GLUE_AWS_EXTERNAL_ID` → trust policy roli
zaktualizowana na docelowe wartości (placeholder zastąpiony prawdziwym
principal Snowflake).

**Pytania/odpowiedzi:**
- *Dlaczego rola, skoro dotąd tworzyłem tylko userów?* — user = trwałe
  poświadczenia (access key) do WŁASNEGO, długoterminowego użytku; rola = brak
  własnych trwałych poświadczeń, tymczasowe pożyczenie uprawnień przez
  `AssumeRole` (token ~1h, sam wygasa) — standardowy wzorzec cross-account
  access, gdy dostęp potrzebuje ZEWNĘTRZNY system (tu: Snowflake), nie chcesz
  dzielić się trwałym sekretem.
- *Czy rola "przepada" po jednym użyciu?* — nie, rola istnieje trwale jak user;
  jednorazowy/tymczasowy jest tylko TOKEN wydawany przy każdym `AssumeRole`,
  nie sama rola — Snowflake będzie assumował tę rolę przy każdym zapytaniu,
  za każdym razem dostając nowy, krótkożyjący token.
- *Policy vs rola — co jest pod czym?* — policy to martwy dokument (lista
  uprawnień), nic nie robi sam z siebie, dopóki nie jest przypięty do
  tożsamości (user/rola/grupa); rola = pojemnik złożony z DWÓCH części: trust
  policy ("kto może mnie assumować" = authn) + przypięta permissions policy
  ("co wolno po wejściu" = authz).
- *Czy to jest authentication i authorization?* — tak: trust policy = AuthN
  (czy w ogóle wpuszczam), permissions policy = AuthZ (co wolno dotknąć);
  zastrzeżenie: w AWS `AssumeRole` oba kroki zlewają się w jedno wywołanie API
  (nie są tak ostro rozdzielone jak np. w klasycznym OAuth), ale koncepcyjny
  podział jest identyczny — ta sama uniwersalna zasada co w cheat sheet 14.6.
- *Czy jawne wpisywanie AWS Account ID w ARN-ach jest niebezpieczne?* — nie,
  Account ID to publiczny identyfikator (jak NIP), nie sekret — sam w sobie nie
  daje dostępu. Prawdziwie wrażliwe są access/secret keys. Tak samo robi się to
  na produkcji (Terraform/CloudFormation z Account ID jako zwykłą zmienną, nie
  secretem).
- *Dlaczego `Principal` w trust policy zmienia się z placeholdera na
  `GLUE_AWS_IAM_USER_ARN`?* — kolejność wymuszona zależnością czasową: Snowflake
  wymaga ARN JAKIEJŚ roli już przy `CREATE CATALOG INTEGRATION`, ale własnego,
  docelowego IAM usera (do którego trust policy ma finalnie zaufać) generuje
  DOPIERO w trakcie tworzenia integracji — poznajemy go dopiero z `DESCRIBE`.
  Placeholder nie był nigdy docelową wartością, tylko technicznym wymogiem
  "trust policy nie może być pusta".
- *Po co external ID skoro jest już ARN?* — Snowflake używa TEGO SAMEGO
  wewnętrznego IAM usera dla wielu swoich klientów naraz ("Snowflake provisions
  a single IAM user for your entire Snowflake account") — external ID chroni
  przed confused deputy problem (inny klient Snowflake, znający ten sam ARN,
  nie mógłby assumować akurat TEJ roli bez znajomości TEGO konkretnego ID).

**Status:** catalog integration gotowa. Następny krok planu — external volume
(S3, druga rola z tym samym dwuetapowym wzorcem trust policy), potem
`CREATE DATABASE ... LINKED_CATALOG` (auto-discovery 6 tabel).

## 15. Faza 4 — external volume, catalog-linked database, weryfikacja na żywych danych

**Zrobione:** IAM policy `snowflake-s3-external-volume-read` (read-only:
`GetObject`/`ListBucket`/`GetBucketLocation`) → rola
`snowflake-s3-external-volume-role` z własnym, jawnie wybranym external ID
(nie generowanym przez Snowflake, w odróżnieniu od catalog integration) →
`CREATE EXTERNAL VOLUME portfolio_ecommerce_ext_vol` (`ALLOW_WRITES = FALSE`,
zgodnie z decyzją: Snowflake tylko czyta, jedynym pisarzem do tych 6 tabel
zostaje Spark) → `DESC EXTERNAL VOLUME` → trust policy domknięta →
`SYSTEM$VERIFY_EXTERNAL_VOLUME` → `PASSED` (pola read/write/list/delete
`UNVERIFIED`, oczekiwanie przy `ALLOW_WRITES = FALSE` — funkcja nie może
wykonać round-trip testu zapisu, to nie błąd).

**Napotkany i naprawiony bug (realny, empiryczny — ten sam charakter co
timestamp/Decimal w Fazie 3):** pierwsza próba `CREATE DATABASE ...
LINKED_CATALOG` zwróciła błąd `unable to load table 'dim_customer' metadata.
Verify the catalog integration has read access`, mimo poprawnej roli Glue.
Przyczyna: załadowanie metadanych tabeli przez Glue Iceberg REST endpoint to
DWUETAPowa operacja — Glue zwraca tylko WSKAŹNIK do pliku `metadata.json`
(`GetTable`), ale REST endpoint sam musi potem fizycznie OTWORZYĆ ten plik w
S3, żeby zwrócić treść. Rola `snowflake-glue-catalog-role` miała tylko
uprawnienia Glue, brakowało `s3:GetObject` na buckecie. Naprawa: dodany drugi
statement (`SnowflakeGlueRestMetadataRead`) do policy
`snowflake-glue-catalog-read`, dający tej samej roli też odczyt S3 — mimo że
`ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS` sugerowałoby, że CAŁY
dostęp do S3 idzie przez external volume, w praktyce catalog integration
(Glue) i external volume (dane) to dwa NIEZALEŻNE ścieżki dostępu do S3, obie
wymagane.

**`CREATE DATABASE portfolio_ecommerce_db LINKED_CATALOG = (...) EXTERNAL_VOLUME
= (...)`** — sukces po poprawce. `SHOW ICEBERG TABLES` pokazuje wszystkich 6
tabel (auto-discovery zadziałało, zero ręcznych `CREATE ICEBERG TABLE`).
`SELECT COUNT(*) FROM dim_customer` → **51** — zgodne z oczekiwaniem (50
klientów + 1 wiersz historii SCD2 z Fazy 3). Trzeci niezależny silnik (po
Sparku i Athenie) czytający ten sam Glue Catalog/S3 — potwierdzenie tezy
multi-engine z ADR-002/cheat sheet 15.1.

**Pytania/odpowiedzi:**
- *Dlaczego `SHOW ICEBERG TABLES`, nie zwykłe `SHOW TABLES`?* — tabela Iceberg
  to osobny typ obiektu w modelu Snowflake (jak `VIEW`/`EXTERNAL TABLE`), ma
  właściwości których natywna tabela nie ma (`EXTERNAL_VOLUME`, `CATALOG`) i
  brakuje jej typowego Time Travel Snowflake (bo historię/wersjonowanie i tak
  daje już sam format Iceberg) — wystarczająco różne, żeby być odrębną
  kategorią, nie wariantem z flagą.
- *Czy to poprawne, że `customer_id=5` powtarza się w wyniku (2 wiersze)?* —
  tak, to sedno SCD Type 2, nie błąd. Klucz biznesowy CELOWO się powtarza;
  rzeczywista unikalność (to, po czym działa `MERGE`) to kompozyt
  `(customer_id, valid_from)` — bez osobnej kolumny surrogate key ten kompozyt
  JEST kluczem tabeli. `is_current=true` gwarantuje jedną aktualną wersję per
  klient, ale samo w sobie nie jest unikalne (wielu różnych klientów ma
  `is_current=true` jednocześnie). Pełne rozwinięcie (w tym rola surrogate key
  w klasycznym Kimballu) przeniesione do cheat sheeta, sekcja 5.3.
- *Dlaczego rola do catalog integration potrzebuje i Glue, i S3 — Glue jako
  spis treści, S3 jako dane+metadane+manifesty?* — dokładnie trafna intuicja,
  potwierdzona: Glue trzyma TYLKO wskaźnik do aktualnego `metadata.json`
  (`GetTable`), nie zna/nie parsuje jego treści; samą treść (i dalej manifest
  list → manifest file → dane) silnik czyta bezpośrednio z S3. Pełne
  rozwinięcie z analogią "spis treści vs same książki" przeniesione do cheat
  sheeta, sekcja 15.1.1.

**Status:** read path Fazy 4 w pełni zweryfikowany na żywych danych. Zostały
punkty 6-9 planu: RBAC, dynamic masking, row access policy, Query Profile.
Odnotowana też (nie wdrożona jeszcze) decyzja: warstwa Gold/marty (przyszła
faza, dbt) pójdzie w "Szkołę C" — Snowflake-managed Iceberg tables
(`CATALOG = 'SNOWFLAKE'`, osobny external volume z `ALLOW_WRITES = TRUE`) —
zamiast natywnych tabel Snowflake, żeby uniknąć vendor lock-in na poziomie
warstwy serwującej (ten sam motyw co "$2.5M savings przez vendor independence"
z P&G w cheat sheecie), zachowując przy tym pełną wydajność/funkcje Snowflake
(masking/RLS potwierdzone jako działające wprost na tabelach Iceberg, także
externally-managed). Decyzja do formalnego ADR dopiero przy starcie fazy dbt.

## 16. Faza 4 — RBAC + Dynamic Data Masking (punkty 6-7 planu)

**Zrobione (RBAC):** role `portfolio_analyst`/`portfolio_engineer` z
dziedziczeniem (`GRANT ROLE portfolio_analyst TO ROLE portfolio_engineer`),
wpięte pod wbudowaną hierarchię (`GRANT ROLE portfolio_engineer TO ROLE
SYSADMIN`), granty na bazę/schemat/tabele (w tym `ON FUTURE TABLES`) +
warehouse. Test przełączania (`USE ROLE` + `SELECT COUNT(*)`) → obie role
zwracają 51, dziedziczenie działa.

**Zrobione (masking):** `CREATE MASKING POLICY email_mask` na `dim_customer.
email` (pełny email dla `portfolio_engineer`/`ACCOUNTADMIN`, `***@domena` dla
`portfolio_analyst`) → `ALTER ICEBERG TABLE ... MODIFY COLUMN email SET
MASKING POLICY`. Test na żywo: `portfolio_analyst` → `***@...` potwierdzone.

**Napotkane i naprawione bugi (dwa, oba realne/empiryczne):**
1. `CREATE MASKING POLICY` → `This session does not have a current database` —
   brak `USE DATABASE`/`USE SCHEMA` przed tworzeniem obiektu; naprawa: jawny
   kontekst sesji.
2. Po naprawie #1 → `This operation is not supported in a catalog-linked
   database` — catalog-linked database (`portfolio_ecommerce_db`) obsługuje
   TYLKO tworzenie schematów/tabel Iceberg/database roles, nie może hostować
   żadnych innych obiektów Snowflake (w tym polityk). Naprawa: nowa, zwykła
   (nie linked) baza `portfolio_governance.policies` wyłącznie na obiekty
   governance; polityka POWSTAJE tam, ale APLIKOWANA jest na kolumnie w
   `portfolio_ecommerce_db` przez pełną kwalifikowaną nazwę — to jest
   wspierane i zgodne z dokumentacją Snowflake.

**Pytania/odpowiedzi:**
- *Czy dane nadal są tylko w S3, `portfolio_governance` to miejsce na dane?* —
  nie, dane (6 tabel) fizycznie w 100% zostają w S3 bez zmian;
  `portfolio_governance` trzyma wyłącznie definicje polityk (logika SQL, nie
  wiersze) — ten sam rodzaj miejsca co definicje ról/grantów, nieporównywalny
  rozmiarowo do prawdziwych danych biznesowych. Trzy niezależne warstwy: dane
  (S3) / governance (Snowflake, `portfolio_governance`) / RBAC (Snowflake,
  poziom account) — każda z innego powodu w innym miejscu.
- *Czy widzę utworzone role w bazie danych?* — nie, role (`portfolio_analyst`
  itd.) to obiekty na poziomie CAŁEGO KONTA (`SHOW ROLES`), niezwiązane z
  żadną konkretną bazą — inaczej niż "database role" (osobny typ obiektu w
  Snowflake, zamknięty wewnątrz jednej bazy, i to on akurat JEST dozwolony w
  catalog-linked database). Projekt świadomie używa zwykłych ról account-level
  (prostsze przy tej skali — 2 role, jeden zespół), database roles odnotowane
  jako alternatywa, nie wdrożone.
- *Kiedy masking, kiedy RLS?* — test decyzyjny: "czy ta osoba w ogóle powinna
  wiedzieć, że wiersz istnieje?" — tak, ale nie wszystkie detale → masking
  (kolumna); nie, cały rekord poza zasięgiem → RLS (wiersz znika z wyniku).
  Pełne rozwinięcie z przykładami (multi-tenant SaaS, nauczyciel/klasa,
  numer karty płatniczej, pensja) przeniesione do cheat sheeta, sekcja 14.4.

**Status:** punkty 6-7 planu zamknięte i zweryfikowane. Następny: punkt 8 —
row access policy na `dim_customer.country` (symulacja: `portfolio_analyst`
widzi tylko `country = 'DE'`, `portfolio_engineer`/`ACCOUNTADMIN` widzą
wszystko — symetrycznie do maskowania).

## 17. Faza 4 — RLS, Query Profile, zamknięcie fazy

**Zrobione (RLS, punkt 8):** `CREATE ROW ACCESS POLICY country_rls` w
`portfolio_governance.policies` (ten sam wzorzec co masking — polityka nie
może powstać w catalog-linked database) → `ALTER ICEBERG TABLE dim_customer
ADD ROW ACCESS POLICY ... ON (country)`. Test: `portfolio_analyst` → 2 wiersze
(tylko `country='FR'`... nie, `country` filtrowany do `DE` w definicji, wynik
zależny od rzeczywistych danych), `portfolio_engineer` → 51 (bez zmian).
Potwierdzone działanie.

**Zrobione (Query Profile, punkt 9):** dwa napotkane, kolejne realne "zygzaki"
z cache'em, zanim dotarliśmy do prawdziwego skanu:
1. Pierwsze wykonanie zapytania → Query Profile pokazał `QUERY RESULT REUSE
   [0] 100%` zamiast realnego skanu — Snowflake oddał wynik z 24h persisted
   result cache, w ogóle nie dotykając S3/Iceberga.
2. `ALTER SESSION SET USE_CACHED_RESULT = FALSE` odpalone jako OSOBNE
   wykonanie (nie "Run All" razem z `SELECT`) → nie zdążyło wejść w życie,
   drugi test też trafił w cache.
3. Naprawa: `ALTER SESSION` + `SELECT ... AND 1=1` (celowa zmiana tekstu
   zapytania jako dodatkowe zabezpieczenie) zaznaczone i odpalone razem →
   realne wykonanie, `Total execution time: 121ms`, status "Success"/
   "Finished", NIE "Result Reuse".

**Dowód końcowy:** węzeł `DynamicSecureView` z `Object name: "DIM_CUSTOMER (+
RowAccessPolicy)"` w drzewie Query Profile — RLS wstrzyknięta w plan
wykonania (tabela przepisana jako widok z polityką), nie doklejona do wyniku
po fakcie. Szczegółowe statystyki (bytes/partitions scanned) niedostępne przy
tak małej tabeli (0% czasu na węzeł) — oczekiwane przy tej skali, nie błąd;
`Query Details` (zakładka obok Profile) pokazuje podsumowanie zamiast tego.

**Dodatkowa obserwacja (poza planem, warta odnotowania):** RLS/masking
zdefiniowane w Snowflake **NIE działają automatycznie w Athenie** — Athena
czyta bezpośrednio z Glue+S3, z pominięciem silnika Snowflake, więc nie zna
ani nie egzekwuje tych polityk. Governance jest natywnie jednosilnikowe
(przywiązane do Snowflake), w odróżnieniu od FORMATU danych (Iceberg —
faktycznie multi-engine). Istnieje mechanizm to naprawiający (Snowflake
Horizon Iceberg REST Catalog + Scan Plan API, server-side policy enforcement
dla zewnętrznych silników) — GA dla Sparka, dojrzałość dla innych silników
(w tym Athena) niepotwierdzona/zmienna. Świadomie NIE wdrożone w tym
projekcie — odnotowane jako luka do wspomnienia na rozmowie, nie do naprawy
teraz.

**Pytania/odpowiedzi:**
- *Co to jest Query Profile, kiedy się go używa?* — wizualne drzewo
  operatorów wykonania zapytania (odpowiednik `EXPLAIN ANALYZE` z Postgresa,
  cheat sheet 3.1), używane reaktywnie (debug wolnych zapytań — który węzeł
  zjada czas) lub proaktywnie przy istotnych zmianach architektury
  (weryfikacja że optymalizacja/governance faktycznie działa, nie tylko
  "wydaje się" działać) — nie rutynowo przy każdym zapytaniu.

**Status: FAZA 4 ZAMKNIĘTA.** Wszystkie 9 punktów planu zweryfikowane na
żywych danych: catalog integration (REST/Glue), external volume, RBAC z
dziedziczeniem, masking, RLS, Query Profile jako dowód. Dwie decyzje do
przyszłych faz odnotowane, nie wdrożone: "Szkoła C" (Snowflake-managed
Iceberg dla Gold w dbt) i mechanizm Horizon IRC dla governance w Athenie
(jeśli kiedyś uzasadnione). Następny krok: Faza 5 (dbt) — pierwszy moment
przejścia na formalny workflow Claude Code + Codex z ADR-003 (patrz też
ADR-003, sekcja retrospektywna).

## 18. Faza 5 — start (dbt), decyzje projektowe + duży blok teorii przeniesiony do cheat sheeta

**Plan Fazy 5 ustalony:** decyzje projektowe (tu) → scaffolding dbt (Claude Code)
→ staging models → drugi external volume z zapisem (`ALLOW_WRITES = TRUE`, pod
"Szkołę C") → marty jako Snowflake-managed Iceberg → testy + Codex review.

**Decyzje podjęte:**
- **SCD2 zostaje w Icebergu (Spark MERGE z Fazy 3), dbt NIE robi `dbt
  snapshot` na tych samych danych.** Uzasadnienie: unika podwójnej/duplikowanej
  historii, zachowuje multi-engine value (SCD2 w Icebergu widoczne dla
  Sparka/Athena/Snowflake jednakowo; SCD2 w dbt snapshot żyłby wyłącznie w
  Snowflake jako natywna tabela), spójne z zasadą już zapisaną w cheat sheet
  5.3 ("jedna warstwa odpowiedzialna za historię"). dbt tylko CZYTA gotowe
  SCD2 (`WHERE is_current = true` / point-in-time join).
- **Brak osobnego `dim_geography`.** `country` to płaski, pojedynczy atrybut
  bez hierarchii (brak miasta/regionu w `seed.py`) — wydzielenie go byłoby
  nadmierną normalizacją; zostaje jako degenerate attribute w `dim_customer`.

**Duży blok Q&A o fundamentach dbt/ELT/multi-engine** (dbt czym jest i po co,
orkiestracja vs transformacja/ADF vs Airflow vs dbt, deklaratywne vs
imperatywne, korekta uproszczenia Bronze/Silver/Gold, Spark vs dbt przy dużej
skali, natywne alternatywy dla dbt w Snowflake, DLT/Lakeflow i dbt-databricks,
Snowpark jako świadomie odrzucona alternatywa, cel multi-engine w firmie) —
**w całości przeniesiony do cheat sheeta, nowa sekcja 25** (7 podsekcji),
żeby nie duplikować obszernej, ogólnej teorii w dzienniku projektu. Tu tylko
odnotowanie, że temat był i gdzie szukać.

**Status:** decyzje Fazy 5 ustalone, gotowość do przygotowania `CLAUDE.md` i
pierwszego zadania scaffoldingu w Claude Code.

## 19. Faza 5 — pierwszy moment przejścia na Claude Code, staging w pełni działający

**Setup narzędziowy:** VSCode + rozszerzenie Claude Code (terminal wewnątrz
VSCode, nie osobne okno — ten sam shell/`.venv` co dotąd, zero nowego
środowiska), zalogowany kontem Claude.ai. `CLAUDE.md` przygotowany po
angielsku (patrz plik w repo) — kontekst projektu, decyzje wiążące (SCD2 w
Icebergu, brak snapshotu, Gold jako Snowflake-managed Iceberg), konwencje
kodu, guardraile (brak `DROP`/sekretów bez potwierdzenia). Hooks
(`.claude/settings.json`) świadomie jeszcze NIE skonfigurowane — do zrobienia
później; do tego czasu tryb ręcznej akceptacji edycji pełni tę rolę.
Przegląd VSCode extensions dla DE (dbt Power User, SQLFluff, Ruff, Better
Jinja, AWS Toolkit itd.) i teoria governance AI-agentów w korpo (permission
mode vs uprawnienia do danych, hooks jako defense-in-depth, audyt) —
**przeniesione do cheat sheeta, nowa sekcja 26**.

**Sesja 1 Claude Code — scaffolding + staging (zadanie: tylko staging, bez
martów):** Plan Mode → dwa pytania architektoniczne do usera (gdzie
hostować staging — nowa baza `portfolio_analytics`, potem przemianowana na
**`portfolio_dbt`** żeby uniknąć pomyłki z rolą `portfolio_analyst`; jak
obchodzić wielkość liter identyfikatorów z Glue — zweryfikować empirycznie,
nie zgadywać) → plan zapisany do pliku, zaakceptowany w trybie ręcznej
akceptacji edycji (nie Auto — pierwsza sesja piszącą pliki, brak jeszcze
hooków).

**Wykonanie:** `uv add dbt-core dbt-snowflake` (już były w `pyproject.toml`,
zweryfikowane empirycznie mimo to) → `dbt init --skip-profile-setup` →
usunięcie boilerplate, **celowe usunięcie `snapshots/`** z komentarzem w
`dbt_project.yml` odsyłającym do `CLAUDE.md` decyzji 1 → `dbt_project.yml`
(`materialized: view`, `schema: staging`) → `profiles.yml.example` (template,
key-pair auth, wszystkie wartości przez `env_var()`, `profiles.yml` dopisane
do `.gitignore`) → `schema.yml` (source `portfolio_ecommerce_db.
portfolio_ecommerce`, `quoting` z komentarzem uzasadniającym, testy
unique+not_null; poprawna samodzielna korekta: `data_tests:` zamiast
przestarzałego `tests:` w dbt 1.12) → 6 modeli `stg_*.sql` (rename/cast,
`WHERE is_current = TRUE` dla dim_*, komentarz w każdym pliku odsyłający do
`CLAUDE.md decision 1`) → weryfikacja offline (`dbt parse`/`dbt ls`/`dbt
compile --no-introspect` z fałszywymi env vars w scratchpadzie poza repo,
`git check-ignore` na `profiles.yml`/`target/` — potwierdzone ignorowane).

**Realna infrastruktura dorobiona przez użytkownika (poza Claude Code):**
`CREATE DATABASE portfolio_dbt` + granty `USAGE`/`CREATE SCHEMA` dla
`portfolio_engineer` → test `SELECT "customer_id" FROM ..."dim_customer"` →
**zadziałał bez zmian** (quoting z planu był poprawny za pierwszym razem,
ryzyko z planu rozstrzygnięte empirycznie) → klucze RSA (`openssl genrsa
2048` + konwersja PKCS8, Snowflake wymaga RSA, nie Ed25519) → `ALTER USER
... SET RSA_PUBLIC_KEY` → `~/.dbt/profiles.yml` + zmienne środowiskowe
(`DBT_SNOWFLAKE_ACCOUNT` w formacie `<ORGANIZACJA>-<KONTO>`, np.
`VIOWCJF-OO15773`) → `dbt debug` → **`All checks passed!`**.

**Wynik końcowy:**
```
dbt run --select staging  → PASS=6  ERROR=0
dbt test --select staging → PASS=17 ERROR=0
```
Wszystkie 6 widoków w `portfolio_dbt.dbt_dev_staging`, wszystkie testy
(unique+not_null na PK, not_null na FK) zielone za pierwszym razem.

**Pytania/odpowiedzi (reużywalna teoria — pełne rozwinięcie w cheat sheet
26):** permission mode Claude Code vs uprawnienia do danych w korpo (dwie
osobne warstwy), hooks jako mechaniczny guardrail niezależny od tego co model
"postanowi", VSCode setup (Python już był, dbt to zwykły pakiet nie osobny
CLI, terminal w VSCode = ten sam terminal co systemowy, `.venv` jako
"Workspace" interpreter), dlaczego `VIEW` a nie `TABLE`/materialized/temp dla
stagingu (tanie, zawsze aktualne, staging jest "cienki"), Bronze/Silver/Gold
vs staging/marts (dwie równoległe konwencje, nie sprzeczne), RSA vs Ed25519 i
konkretne komendy `openssl`.

**Status:** staging w pełni działający i zweryfikowany na żywych danych.
Następny krok: Codex (niezależny review diffu tej sesji), potem external
volume Gold (`ALLOW_WRITES = TRUE`) i marty jako Snowflake-managed Iceberg.

## 20. Faza 5 — Codex review sesji 1, oba findingi zamknięte

**Codex (agent w VSCode, sesja niezależna od Claude Code)** dostał: oryginalne
zadanie + polecenie "nie implementuj od nowa, zrób adwersarialny review
względem zadania i `CLAUDE.md`". Odpowiedział (po angielsku) dwoma
findingami, oba potraktowane poważnie, nie odrzucone z automatu:

- **P1 — konwersja timestampów.** `CONVERT_TIMEZONE('UTC', col)::
  TIMESTAMP_NTZ` w 6 modelach zakłada, że źródłowe kolumny to
  `TIMESTAMP_LTZ` (zgodnie z komentarzem Claude Code: "Spark timestamptz →
  Snowflake surfaces as LTZ"); Codex słusznie zauważył, że to było
  ZAŁOŻENIE, nie zweryfikowany fakt — dla `TIMESTAMP_NTZ` ta sama komenda
  przesunęłaby wartości błędnie. **Zweryfikowane empirycznie:** `DESCRIBE
  TABLE dim_customer` → `valid_from`/`valid_to` faktycznie `TIMESTAMP_LTZ(6)`.
  Założenie Claude Code było poprawne, kod bez zmian. Dobry instynkt Codexa
  jako reviewera (kwestionowanie niezweryfikowanego założenia), nawet mimo że
  wynik potwierdził oryginalną implementację.
- **P2 — case sensitivity identyfikatorów.** Codex tego nie mógł wiedzieć
  (nie miał dostępu do wyniku terminala), ale `dbt test` już wcześniej
  przeszedł `PASS=17` z tym dokładnie quotingiem — empirycznie rozstrzygnięte
  przed review, odrzucone bez zmian kodu.

**Domknięcie pętli:** wynik `DESCRIBE TABLE` przekazany z powrotem do Codexa;
potwierdził oba findingi jako zamknięte, zero zmian w SQL potrzebnych,
`dbt test PASS=17` spełnia wymóg `CLAUDE.md` (`dbt run`+`dbt test` po zmianie
modeli — tu: po review). Codex nie trafił na żaden realny błąd swojej/cudzej
implementacji → nic do `ai/failure-log/`.

**Propozycja Codexa (czeka na decyzję):** dopisać jednolinijkowe uzasadnienie
LTZ wprost w komentarzach modeli (dziś jest tylko w planie, nie w kodzie) i
zamienić spekulatywny komentarz o `CASE_INSENSITIVE` w `schema.yml` na
zweryfikowany fakt. Kosmetyka dokumentacji, zero zmian w SQL — niezrobione,
czeka na "yes/no" w kolejnej sesji.

**Status: Sesja 1 Claude Code (staging) w pełni zamknięta** — zaimplementowana,
przetestowana na żywo, zweryfikowana niezależnym review, oba findingi
rozstrzygnięte empirycznie. Następny krok: external volume Gold
(`ALLOW_WRITES = TRUE`), potem marty jako Snowflake-managed Iceberg
(Sesja 2 Claude Code).

---

## 21. Faza 5 — nazewnictwo warstw i rename tabel Iceberg (ADR-005)

**Trigger:** pytania "na którym etapie jestem" (Bronze/Silver/Gold vs staging/marts)
i uwaga, że `dim_`/`fact_` na tabelach przed dbt myli: w praktyce i w konwencji dbt
oznaczają finalny Gold pod Power BI (w Postgresie użytkownik trzymał osobne bazy
bronze/silver/gold). Krytyka słuszna: nazewnictwo było nietypowe, a marty kolidowałyby
nazwami ze źródłem. Wcześniejsza rekomendacja "zostawić, bo koszt przepisania" została
cofnięta: koszt mylącej nazwy płaci się przy każdym onboardingu, rename tylko raz.

**Druga opinia (ChatGPT):** `dim_`/`fact_` przed Gold nie jest uniwersalnym
antywzorcem (na rozmowie mówić "konwencja, nie prawo"), ale w tej architekturze nazwa
komunikuje złą granicę. Ryzyko renamu leży w tożsamości obiektu w katalogu, zależnościach
downstream i governance, nie w danych. Opcja C (osobny namespace) dobra, ale droższa niż
się wydaje (nazwa bazy Glue w IAM, catalog integration, catalog-linked DB, Spark, dbt).
Najcenniejsza uwaga: dlaczego model wymiarowy istnieje i przed dbt, i potencjalnie
ponownie w Gold? To wymusiło regułę "obiekt w Gold tylko jeśli dodaje logikę".

**Decyzja (ADR-005):** opcja A. Nowe nazwy: `customer`, `product`, `orders`,
`order_items`, `payments`, `order_status_history`. `dim_`/`fct_` tylko dla martów.

**Wykonanie (Claude Code, Plan Mode, ręczna akceptacja edycji):** plan przejrzany
zawczasu (jar `iceberg-spark-runtime-3.5_2.12-1.9.1` z `GlueCatalog.renameTable`
i `RegisterTableProcedure`, PySpark 3.5.5). Snapshot przed → pilotaż na `dim_product`
(bez polityk) → pozostałe 5 po jednej z kontrolą → snapshot po. Wynik 6/6:
`customer` 51, `product` 30, `orders` 200, `order_items` 502, `payments` 160,
`order_status_history` 493 wiersze; `metadata_location` i ścieżki S3 identyczne.
Fallback `register_table` niepotrzebny, żadnego `DROP`. Kod zaktualizowany (tylko nazwy
tabel): `merge_dimensions.py`, `merge_facts.py`, `schema.yml`, 6 × `stg_*.sql`; grep starych
nazw pusty; `dbt ls` = 6 modeli / 6 źródeł / 17 testów.

**Własne poprawki w trakcie:** (1) mój pierwszy prompt kazał robić rename przez
boto3/CLI, a Glue nie ma natywnego renamu (Iceberg robi create+drop wpisu w katalogu),
właściwa droga to Spark; (2) twierdziłem, że polityki masking/RLS "podążą" za tabelą,
to było zgadywanie: Snowflake widzi rename jako nowy obiekt, polityki trzeba założyć
ponownie (Claude Code doszedł do tego samego niezależnie).

**Potwierdzone przez użytkownika po renamie (bez zachowanego outputu):** Snowflake
(synchronizacja catalog-linked DB, granty, ponowne założenie masking/RLS na `customer`)
oraz `dbt run --select staging` i `dbt test --select staging` przeszły poprawnie.
Konkretne liczby (oczekiwane PASS=6 i PASS=17) i wynik testu ról nie zostały
zachowane, więc zapis opiera się na deklaracji użytkownika, nie na wklejonym wyniku.

**Znany koszt:** katalogi S3 zachowały stare nazwy (`.../dim_customer/`); przyszła
tabela utworzona pod starą nazwą w domyślnej lokalizacji trafiłaby do tego samego
katalogu. Świadomie przyjęte.

## 22. Faza 5 — external volume Gold gotowy

**Zrobione (ten sam dwuetapowy wzorzec co Faza 4, teraz z zapisem):** IAM policy
`snowflake-s3-gold-write` (`PutObject`/`GetObject`/`GetObjectVersion`/`DeleteObject`/
`DeleteObjectVersion` na `s3://tp-portfolio-ecommerce-raw/gold/*`, `ListBucket` z
warunkiem `s3:prefix=gold/*`, `GetBucketLocation` osobno) → rola
`snowflake-s3-gold-write-role` z własnym external ID (`portfolio_gold_ext_vol_eid`)
→ `CREATE EXTERNAL VOLUME portfolio_gold_ext_vol` (`ALLOW_WRITES = TRUE`,
`STORAGE_BASE_URL = s3://.../gold/`) → trust policy domknięta na
`STORAGE_AWS_IAM_USER_ARN` → `GRANT USAGE ... TO ROLE portfolio_engineer`.

**Weryfikacja:** `SYSTEM$VERIFY_EXTERNAL_VOLUME` → `writeResult`/`readResult`/
`listResult`/`deleteResult` wszystkie `PASSED` (w odróżnieniu od Fazy 4, gdzie przy
`ALLOW_WRITES = FALSE` te pola były `UNVERIFIED` — teraz funkcja mogła wykonać pełny
round-trip). `STORAGE_AWS_IAM_USER_ARN` identyczny jak dla
`portfolio_ecommerce_ext_vol` z Fazy 4 (`arn:...:user/g6702000-s`) — potwierdza, że
Snowflake ma jednego wspólnego IAM usera na konto; rozróżnieniem między wolumenami
jest wyłącznie external ID, nie osobny user.

**Napotkany błąd:** `CREATE EXTERNAL VOLUME` → "SYSADMIN must have CREATE EXTERNAL
VOLUME granted" — aktywna rola w Worksheecie była `SYSADMIN`, nie `ACCOUNTADMIN`;
naprawa: `USE ROLE ACCOUNTADMIN`.

**Pytania/odpowiedzi:** dlaczego osobna rola IAM per wolumen zamiast jednej (zakres
uprawnień jest różny — read-only na całym buckecie vs read-write tylko na `gold/`;
wspólna rola musiałaby sumować oba, łamiąc least privilege), co to jest ARN.

**Status:** infrastruktura pod Gold kompletna. Następny krok: Sesja 2 Claude Code —
pierwszy mart (`fct_customer_revenue`, Snowflake-managed Iceberg, `CATALOG =
'SNOWFLAKE'`, ten external volume), point-in-time join do SCD2, potem Codex review.

---

**Sprostowanie (Great Expectations):** w rozmowie podałem błędnie, że Great Expectations
jest zrobione w Fazie 2. Faktycznie Faza 2 to własny skrypt `validate.py` (kod bez frameworka, napisany w rozmowie z Claude, jak reszta Faz 1-4, zob. ADR-003)
(schemat, not-null, unikalność PK w batchu, accepted values, kwarantanna w
`quarantine/`), a GE jest świadomie odłożony jako zakres Advanced (sekcja 8 tego
dziennika i docstring w `validate.py`). ADR-002 nadal wymienia GE jako plan, więc przy
zmianie jego statusu z Proposed na Accepted dopisać, że ten etap wdrożono jako
`validate.py`. `CLAUDE.md` i cheat sheet skorygowane.

**Pytania/odpowiedzi (teoria przeniesiona do cheat sheeta, 15.1.2 i 25.9):** po co
staging (DRY: "posprzątaj raz, użyj wszędzie"), Bronze/Silver/Gold to etykiety
warstw danych, a staging/marts to podział pracy wewnątrz dbt, czym jest mart
(odpowiedź na pytanie biznesowe łącząca kilka staging; "5-10 martów" = tyle, ile
pytań), testy generic vs custom, `target/compiled` vs `target/run`, prosty przykład
"ile wydał każdy klient", dbt w cyklu życia pipeline'u (nie jednorazowy setup:
`dbt run`/`dbt test` odpalane przy każdym cyklu danych, docelowo z Airflow i CI),
mapowanie warstw w projekcie (cheat sheet 25.10).

**Status:** rename w Glue, w kodzie i w Snowflake zakończony i zweryfikowany
(część Snowflake/dbt: deklaracja użytkownika). Następny krok: drugi
external volume (`ALLOW_WRITES = TRUE`) i Sesja 2 (marty, w tym pierwszy
`fct_customer_revenue` z point-in-time joinem do SCD2).

---

## 23. Faza 5 — naprawa dat SCD2 w `customer`, porządek w repo

**Wykrycie:** przed pierwszym martem zapytanie kontrolne (zamówienia wcześniejsze
niż pierwsza wersja klienta) dało 200/200. Diagnoza: wszystkie `valid_from`
klientów w jednym 15-minutowym oknie 26.09, a produkty rozrzucone na 24 dni.
Przyczyna: klienci załadowani wczesną wersją skryptu, która brała czas
przetwarzania zamiast `source.updated_at`. Późniejsza poprawka kodu nie ruszyła
istniejących wersji, bo MERGE ich nie odwiedza. Ten sam błąd był na granicy
wersji klienta 5 (luka ~6 min bez ważnej wersji). Błąd był niewidoczny, dopóki
nic nie czytało tych dat. Wyszedł dopiero przy planowaniu point-in-time joinu.

**Sprostowanie do sekcji 10:** jedna partycja `dim_customer` w Athenie nie
wynikała z „testów SCD2 robionych dzisiaj”, tylko z tego błędu.

**Naprawa (Claude Code, Spark):** MERGE tylko z UPDATE: pierwsze `valid_from` :=
`MIN(created_at)` z surowych plików S3, a granica wersji klienta 5 := `updated_at`
zdarzenia zmiany. Pilot na kliencie 5, potem 49 pozostałych. Weryfikacja: 51
wierszy / 50 klientów / 50 bieżących bez zmian, hashe atrybutów bez zmian,
wersje ciągłe, ponowne uruchomienie nic nie zmienia. Snapshot
`5434959617553012471` → `6395697598619178545` (rollback możliwy). Skrypt i stany
przed/po w `scripts/repairs/2026-09-29_customer_valid_from/`.

**Weryfikacja w Snowflake:** `valid_from` od 2026-06-22 do 2026-09-26.
Zapytanie kontrolne: 115 (rzeczywisty efekt losowych, niezależnych dat w
`seed.py`; obsłuży go reguła w dbt). Masking i RLS przetrwały UPDATE (w
odróżnieniu od renamu): `portfolio_analyst` widzi tylko `DE` i `***@`.
`dbt test --select staging` → PASS=17.

**Porządek w repo:** okazało się, że Fazy 1–5 (Spark, dbt, ADR-y, `validate.py`,
`github_extract.py`, `CLAUDE.md`, ten log) nigdy nie były zacommitowane. 6
branchy z `main` (5 z 6 branchy budowanych na tymczasowym
indeksie, bez ruszania katalogu roboczego), test konfliktów dla wszystkich 15 par, PR-y #1–#6 ze squash merge.
Cheat sheet i plik debugowy zostają lokalnie (`.gitignore`). `gh` zalogowany
osobno, bez tokena pipeline'u (`GITHUB_TOKEN` z `.env` trafił do środowiska
shella; token do ekstrakcji nie powinien mieć prawa zapisu do repo).

**Status:** Silver poprawny, repo kompletne na `main`. Następny krok: Sesja 2 —
pierwszy mart.

---

## 24. Faza 5 — Sesja 2: pierwszy mart `fct_orders` (Gold, Snowflake-managed Iceberg)

**Zrobione (Claude Code, Plan Mode, ręczna akceptacja):**
- `catalogs.yml` (v1, stabilny; v2 w dbt 1.12 eksperymentalny) z katalogiem
  `gold_iceberg` → `CATALOG = 'SNOWFLAKE'`, `portfolio_gold_ext_vol`. Tabela
  ląduje w `s3://.../gold/_dbt/dbt_dev_marts/fct_orders/`.
- Staging `stg_customers`/`stg_products` zwraca teraz WSZYSTKIE wersje SCD2
  (bez filtra `is_current`). Unikalność na `(klucz, valid_from)` przez
  `dbt_utils` 1.4.1 + strażnik „najwyżej jedna wersja bieżąca” (`unique` z
  `where: is_current`).
- `int_customer_versions`: pierwsza wersja klienta obowiązuje od `1900-01-01`
  (obsługa 115 zamówień sprzed pierwszej wersji z `seed.py`), połowicznie
  otwarty przedział `[effective_from, effective_to)`, bez `email` (żadna
  maskowana kolumna nie trafia do Gold).
- `fct_orders` (grain: zamówienie): `country_at_order` (point-in-time) i
  `current_country`, `gross_order_value`, `paid_amount` (tylko `completed`),
  `refunded_amount`, `is_before_first_customer_version`.
- Testy: m.in. ciągłość wersji (granica n = początek n+1, łapie klasę błędu z
  §23), „dokładnie jedna wersja klienta na zamówienie”, uzgodnienie sumy
  `gross_order_value` ze stagingiem.

**Napotkany błąd (przewidziany w planie):** Iceberg w Snowflake odrzuca
`TIMESTAMP_NTZ(9)` (domyślna precyzja Snowflake, nanosekundy) — format Iceberg
przechowuje maksymalnie mikrosekundy. Naprawa: `::TIMESTAMP_NTZ(6)` na
`order_date`. Bez utraty danych, bo źródło i tak ma precyzję 6.

**Governance:** RLS (`country_rls` na `country_at_order`) i maska przez
`post_hook` (tabela jest odtwarzana przy każdym `dbt run`, więc ręczny `ALTER`
by przepadł), granty przez config `grants`. Kolejność w materializacji:
CREATE OR REPLACE → post_hooki → granty, więc analityk nigdy nie dostaje
SELECT na tabelę bez polityk. Celowo bez `copy_grants`.

**Codex review (3 findingi):**
- P1, przyjęty: `current_country` ujawniał kraj spoza zakresu RLS (analityk
  widzi zamówienie z DE, ale też kraj, do którego klient się przeniósł). RLS
  filtruje wiersze, nie chroni innych kolumn w tych wierszach. Naprawa: nowa
  maska `country_mask` na `current_country` (analityk widzi tylko `DE`, inaczej
  NULL).
- P2, przyjęty: `paid_amount` nie odróżniał zwrotu od braku wpłaty → dodany
  `refunded_amount`, poprawiony opis.
- P2, zamknięty bez zmian: `SHOW GRANTS TO ROLE portfolio_analyst` — brak
  dostępu do stagingu i intermediate.

**Wynik końcowy:** `dbt run` 200 wierszy, testy 34/34 (cały łańcuch) i 13/13
(mart po poprawkach). Inżynier: 200 zamówień, 115 sprzed pierwszej wersji
klienta, 44 kraje, 0 NULL. Analityk: 10 zamówień, tylko DE. `POLICY_REFERENCES`
potwierdza obie polityki na tabeli. Ścieżka maski zwracająca NULL nie ma
przykładu w danych (żaden klient nie przeniósł się z DE) — zweryfikowane
tylko podpięcie polityki.

**Ręczne obiekty w Snowflake (poza dbt):** `country_mask` + APPLY dla
`portfolio_engineer`, APPLY na `country_rls`, USAGE na `portfolio_governance`
dla inżyniera, USAGE na `portfolio_dbt` i `dbt_dev_marts` dla analityka.
Zapisane w `snowflake/phase5_gold_governance.sql`.

**Status:** PR #8 (`1add7cd`). Pierwszy mart w Gold działa end-to-end.

---

*Ten plik aktualizuj po każdym większym kroku — kolejność chronologiczna, krótkie Q&A, bez kopiowania całych fragmentów rozmowy.*
