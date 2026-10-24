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

## 25. Porządek w S3: ścieżki tabel Iceberg zgodne z nazwami

**Trigger:** po ADR-005 tabele nazywały się `customer`, `orders` itd., ale w S3
leżały w `dim_customer/`, `fact_orders/`. Uznane za bałagan, naprawione przed
Fazą 6, żeby Airflow od początku działał na ostatecznym układzie. Zmiana
decyzji z ADR-005 (dopisana tam sekcja „Aktualizacja”).

**Metoda (Claude Code, Plan Mode):** `rewrite_table_path` + kopia plików S3 +
`register_table` w tymczasowej bazie Glue `portfolio_ecommerce_relocation`
(niewidoczna dla Snowflake: rola catalog integration ma dostęp tylko do
`portfolio_ecommerce`, potwierdzone `SHOW SCHEMAS`) + zamiana dwoma renamami.
CTAS odrzucony, bo gubi historię snapshotów. Pilot `product`, potem
`order_items`, `order_status_history`, `payments`, `orders`, na końcu
`customer` z odciętym na czas zamiany dostępem analityka.

**Weryfikacja:** dla każdej tabeli wiersze, hash zawartości, snapshoty i time
travel identyczne, zmienia się tylko lokalizacja. `customer`: 10 snapshotów,
punkt rollbacku z §23 nadal działa. W Snowflake po każdej zamianie
`ALTER ICEBERG TABLE ... REFRESH` (bez niego Snowflake dalej czytał stare
metadane), obie role, `dbt build` 51/51.

**Odkrycie:** polityki na `customer` przetrwały, bo nazwa się nie zmieniła.
Rename (§21) gubił polityki, przeniesienie plików z `REFRESH` ich nie gubi.
Doprecyzowane w `CLAUDE.md` (decyzja 6).

**Błędy po drodze:**
- `rewrite_table_path` zapisuje listę plików przez warstwę Hadoopa, a w
  kontenerze brakowało `hadoop-aws`. Złapane przez pilota przed jakimkolwiek
  zapisem. Pierwszy wpis w `ai/failure-log/` (ADR-003).
- Przy okazji wyszło, że widok `stg_products` w Snowflake był sprzed Sesji 2:
  `dbt run --select +fct_orders` nie buduje modeli spoza łańcucha martu.
  Wniosek: po zmianach w kilku modelach pełny `dbt build`.

**Sprzątanie:** usunięte wpisy `*_old_path` i baza tymczasowa w Glue, 685
obiektów w S3 (stare katalogi i `migration-staging/`, bucket bez wersjonowania,
więc nieodwracalnie). Przed usunięciem sprawdzone, że żadna tabela nie wskazuje
na stare katalogi. PR #10.

---

## 26. Faza 6 — start (Airflow): obraz i compose, okno ekstrakcji, oba skrypty extract

**Ograniczenie czasowe:** trial Snowflake kończy się ~27.10. Faza 6 prawie nie
zużywa kredytów (warehouse pracuje tylko w tasku dbt), więc limitem jest czas.
Kroki 6.1–6.3 nie dotykają Snowflake.

**Plan Fazy 6:** 6.1 obraz Airflow → 6.2 docker-compose (montowanie `scripts/` i
`dbt_portfolio/`, sekrety z `.env`, `docker.sock`, nazwany obraz Sparka) → 6.3
refaktor skryptów (config z env vars, okno `--start/--end`) → 6.4 DAG
`ecommerce_pipeline` (`[extract_postgres, extract_github]` → `validate` →
`merge_dimensions` → `merge_facts` → `dbt build`) → 6.5 retry z exponential
backoff, `execution_timeout`, `on_failure_callback` → 6.6 backfill na 3 dniach →
6.7 dokumentacja.

**Decyzje architektoniczne:**
- **Spark poza obrazem Airflow.** Obraz `spark-iceberg` ma już Javę i JAR-y
  Iceberga/AWS. Airflow uruchamia go jako chwilowy kontener przez
  `DockerOperator` (dependency isolation), zamiast kopiować JVM i JAR-y do
  obrazu Airflow. Pierwotny pomysł „rozszerzyć obraz Airflow o PySpark z
  Icebergiem” odrzucony po przeczytaniu `spark-docker/Dockerfile`.
- **dbt w osobnym venv w obrazie Airflow** (`/opt/airflow/dbt_venv`), bo dbt i
  Airflow pinują sprzeczne wersje wspólnych bibliotek.
- **Okno ekstrakcji = data interval z 10-minutową zakładką, bez stanu
  watermarku → ADR-006.** Zmienia plan z docstringu `extract.py` (§5:
  „watermark przejdzie do Airflow Variables”). Uzasadnienie: idempotentne runy,
  backfill bez ręcznego cofania stanu, historia okien w `dag_run`. Zakładka
  zamyka lukę „późnego commitu” (`now()` w Postgresie = start transakcji).
  Duplikaty z zakładki zjada istniejąca deduplikacja przed `MERGE`.

**Zrobione (6.1):** `airflow-docker/Dockerfile` (`apache/airflow:2.9.0-python3.12`,
providery z oficjalnym constraints file, dbt-core 1.12.5 + dbt-snowflake 1.12.1 w
osobnym venv) i `requirements.txt` (providery docker/amazon/postgres bez
pinów, wersje z constraints). Weryfikacja: build 51 s,
`apache-airflow-providers-docker 3.9.2`, `dbt --version` → core 1.12.5, snowflake
1.12.1. `BrokenPipeError` w outpucie to efekt `| head -2`, nie błąd.

**Pytania z tego kroku i czego uczą** (teoria: cheat sheet 31.1–31.4):
- **Plan mówił „rozszerzamy PySpark o Iceberg/AWS” — co to znaczy?** Spark sam
  nie umie czytać Icebergu ani rozmawiać z Glue/S3; potrzebuje dodatkowych
  JAR-ów (`iceberg-spark-runtime`, `iceberg-aws-bundle`). „Rozszerzyć” = wbudować
  je w obraz przy buildzie, zamiast kazać Sparkowi pobierać je z Mavena przy
  każdym uruchomieniu (`spark.jars.packages`). Nasz obraz Sparka już je ma,
  dlatego Spark nie trafił do obrazu Airflow.
- **Czy okno ekstrakcji i jego historia będą w Airflow Variables?** Nie. Airflow
  sam wylicza okno każdego runu z `schedule` i `start_date`
  (`data_interval_start/end`), a historia runów z ich oknami jest w bazie
  metadanych (`dag_run`). Variables zostają na konfigurację, nie na stan.
- **Jak zrobić backfill z zakładką — podać `-s` o 5 minut wcześniej?** Nie.
  `-s/-e` w `airflow dags backfill` wybierają, KTÓRE dni (runy) wykonać;
  zakładkę (10 min wstecz) dolicza sam task w szablonie komendy, więc każdy run
  backfillu dostaje ją automatycznie.

**Zrobione (6.2, 8.10):** `docker-compose.yml`: wspólne zmienne Airflow
(`POSTGRES_SOURCE_HOST=postgres-source`, klucze AWS z `.env`, `AWS_REGION` i
`AWS_DEFAULT_REGION`, `GITHUB_TOKEN`, `HOST_PROJECT_DIR=${PWD}` dla ścieżek
montowania w `DockerOperator`), montowanie `scripts/` i `dbt_portfolio/` w
webserverze i schedulerze, `docker.sock` w schedulerze (LocalExecutor wykonuje
taski w nim), stała nazwa obrazu Sparka `portfolio-spark:latest`.

**Zrobione (6.3, część 1, 8.10):** `extract.py` na `--start/--end` (okno
`[start, end)`, daty bez strefy = UTC, walidacja start < end), config z env vars
z obecnymi wartościami jako domyślnymi, bez pliku stanu. Test tym samym oknem
(8.10) w dwóch środowiskach: z Maca (`uv run`) i z kontenera Airflow (`docker
compose run`): oba 0 wierszy bez błędów, `docker.from_env().ping()` → `True`.
Dwa uruchomienia celowo: Mac sprawdza logikę kodu, kontener sprawdza otoczenie
(sieć, sekrety, pakiety z obrazu, montowanie). Zapis do S3 nie został jeszcze
sprawdzony z kontenera (0 wierszy = brak `put_object`).

**Odkryte 20.10 (po przerwie w pracy):** `docker compose ps -a` pokazał
`airflow-webserver` w stanie `Exited (1)` od 29.09 i scheduler na obrazie
oznaczonym samym `sha256:...`.
- Webserver: padły workery gunicorna (`Some workers seem to have died`, `No
  response from gunicorn master within 120 seconds`), najpewniej brak pamięci w
  Docker Desktop albo uśpienie Maca. Serwisy nie mają `restart:`, więc leżał 3
  tygodnie niezauważony.
- Scheduler: przebudowa obrazu 7.10 przepięła nazwę na nowy obraz, a działający
  kontener został na starym (Docker nie podmienia obrazu w działającym
  kontenerze). Nie widział więc montowań, zmiennych ani `docker.sock` z 6.2;
  test z 8.10 przeszedł tylko dlatego, że `docker compose run` startuje świeżą
  kopię.
- Naprawa: `docker compose up -d --build` (wolumeny Postgresów nietknięte).
  Weryfikacja na działającym schedulerze: widzi `scripts/` i `dbt_portfolio/`,
  `ping()` → `True`; webserver stabilny po kilku minutach.

**Pytania z tych kroków i czego uczą** (teoria: cheat sheet 31.5–31.7):
- **Po co `.env`, skoro `boto3` sam znajduje klucze?** `boto3` szuka
  poświadczeń po kolei: zmienne środowiskowe → `~/.aws/credentials` → rola IAM
  maszyny. Na Macu trafia w `~/.aws`, ale kontener nie widzi Twojego `~/.aws`,
  więc `.env` wkłada klucze do zmiennych kontenera. W korporacji kluczy nie ma
  wcale: serwer/pod dostaje rolę IAM i `boto3` trafia w nią (krok 3).
- **Skoro skrypt robi tylko `import`, skąd biorą się pakiety?** `import` ładuje
  pakiet z Pythona, który uruchamia skrypt, a nie go instaluje. Na Macu to
  `.venv` (z `uv.lock`), w kontenerze obraz Airflow (`requirements.txt`, pisany
  ręcznie, wersje z constraints Airflow). Ten sam kod, dwie instalacje, więc
  test w obu miejscach: Mac sprawdza logikę, kontener otoczenie (sieć, sekrety,
  pakiety, montowanie). W korporacji jedno źródło prawdy: obraz budowany w CI.
- **`airflow-init` miał być jednorazowy, a uruchamia się przy każdym `up` — czy
  jest potrzebny?** Jest krótkotrwały, nie jednorazowy: przy każdym starcie
  sprawdza/migruje schemat bazy Airflow i kończy pracę; realną robotę
  (założenie bazy, użytkownika) zrobił raz. Zostaje jako bramka: webserver i
  scheduler czekają, aż skończy się sukcesem.
- **Dlaczego `spark-iceberg` działa cały czas?** Ma komendę `tail -f /dev/null`
  (nic nie robi, ale się nie kończy), a `up` startuje wszystkie serwisy. Odkąd
  Spark uruchamia `DockerOperator` w chwilowych kontenerach, stały kontener
  jest potrzebny tylko do ręcznej pracy.
- **Dlaczego `docker compose` nie działa w innym folderze, a `docker ps` tak?**
  `docker compose` szuka `docker-compose.yml` w bieżącym folderze (i wyżej), a
  nazwa projektu pochodzi od nazwy folderu; `docker ps` pyta silnik Dockera
  bezpośrednio o wszystkie kontenery. Spoza repo: `-f <plik>` albo `-p
  portfolio-project`.
- **Lekcja z 20.10:** przebudowa obrazu nie podmienia go w działającym
  kontenerze (trzeba `up -d --build`), a serwis bez `restart:` po awarii leży
  niezauważony.

**Plan do końca triala Snowflake (27.10; stan 20.10: $368 z $400, więc
limitem jest czas, nie kredyty):** 20–21.10 `github_extract.py` + DAG bez dbt →
22–23.10 task dbt i pierwszy pełny run → 24–25.10 retry, alert, backfill → 26.10
zebranie dowodów (zrzuty Airflow/Snowflake, wyniki dbt, README), bo po 27.10
kroku dbt na Snowflake nie da się już pokazać na żywo.

**Zrobione 21.10:** PR #12 (`331777c`, squash merge w UI): obraz Airflow,
compose, `extract.py`, ADR-006, `CLAUDE.md`. Po merge'u lokalne sprzątanie:
`git switch main && git pull`, `git branch -D` (duże `-D`, bo po squash merge
git nie rozpoznaje gałęzi jako zmergowanej), `git fetch --prune` usunął 11
nieaktualnych remote-tracking branches po PR #1–#11, `git push origin --delete`
dla gałęzi #12. Zostały tylko `main` i `origin/main`.

**Zrobione (6.3, część 2, 21.10):** `github_extract.py` na `--start/--end`
według ADR-006: `issues` z `since=start` po stronie serwera i `< end` po
stronie klienta (okno sprzed 2008 pomija `since`, bo przy epoce GitHub zwraca
pusty wynik); `pull_requests` od najnowszych: pomija nowsze od okna, zbiera
okno, kończy na pierwszym starszym, a przy wyczerpaniu limitu 20 stron przed
dojściem do początku okna wypisuje ostrzeżenie zamiast cicho zwrócić niepełne
dane; `repositories` i `contributors` jako pełny snapshot; bez pliku stanu.
Logika okna PR-ów sprawdzona na danych testowych przed uruchomieniem.
Test na żywo z działającego schedulera, okno 20.10: repositories 1,
issues 1, pull_requests 177 (strona 1 = PR-y zmienione 21.10, pominięte;
strony 2–4 okno; bez ostrzeżenia o limicie), contributors 410. **Pierwszy
zapis do S3 z kontenera Airflow** (klucze z `.env` działają). Tylko 1 issue,
bo API zwraca bieżący stan: obiekt zmieniony 20.10 i ponownie 21.10 trafia do
okna 21.10 (ograniczenie opisane w ADR-006). Dane z GitHuba zostają w raw,
`merge_*` przetwarzają tylko tabele z Postgresa (uproszczenie z fazy 1).

**Błąd w dokumentacji (mój):** aktualizacje z 20.10 (§26 i cheat sheet
31.5–31.7) zgłosiłem jako zapisane w repo, ale do repo trafiły wersje z 7.10.
Pliki zapisywałem pod tą samą ścieżką tymczasową co 7.10 i przy zapisie poszła
stara kopia. Wykryte 21.10 po rozmiarach plików, naprawione 21.10 z
weryfikacją treści w repo. Wniosek: po każdym zapisie sprawdzać treść, nie
tylko komunikat o sukcesie.

**Pytania z tego kroku i czego uczą** (teoria: cheat sheet 12.12, 15.3.1):
- **Zrobiłem squash merge w UI — co dalej w terminalu, i `switch` czy
  `checkout`?** Lokalny git nie wie o merge'u z GitHuba: `git switch main && git
  pull` (pobiera squash commit), potem usuwasz gałąź `git branch -D` (duże D,
  bo po squash git nie rozpozna jej jako zmergowanej). `switch` robi to samo co
  `checkout` przy gałęziach, ale `checkout` umie też nadpisywać pliki, więc
  `switch` jest bezpieczniejszy.
- **`fetch`, `pull`, `prune` — czym się różnią i skąd stare gałęzie
  `origin/*`?** `fetch` pobiera stan GitHuba do kopii `origin/*` (remote-tracking
  branches), nie ruszając Twoich plików; `pull` = `fetch` + scalenie do bieżącej
  gałęzi. Gałęzie usunięte na GitHubie zostają lokalnie jako `origin/*`, dopóki
  nie zrobisz `git fetch --prune`; u nas usunęło to 11 starych po PR #1–#11.
- **Zwykle robi to gotowy konektor — dlaczego własny skrypt, i co to dlt?**
  Wybór to rachunek kosztu UTRZYMANIA, nie napisania (AI obniża tylko ten
  drugi): Fivetran/Airbyte przy wielu standardowych źródłach (płacisz za
  wiersze, nie piszesz kodu), dlt (biblioteka Pythona: paginacja, stan,
  schemat za Ciebie) gdy chcesz kod bez pisania infrastruktury, własny skrypt
  przy nietypowym źródle albo gdy celem jest pokazanie mechaniki, jak tutaj.
  dlt to nie DLT (Delta Live Tables w Databricks, transformacje).
- **Dlaczego więc nie dlt teraz?** Schowałby paginację i okna, które portfolio
  ma pokazać, a przed końcem triala nie ma czasu. Po 27.10: GitHub na dlt obok
  skryptu i porównanie w ADR-007.

**Zrobione (6.4, część 1, 21.10): DAG `dags/ecommerce_pipeline.py` bez dbt.**
```
extract_postgres ──> has_new_rows ──> validate_raw ──> merge_dimensions ──> merge_facts
extract_github   (równolegle, kończy się na raw)
```
- `extract_*` i `validate_raw` przez `BashOperator` (zależności są w obrazie
  Airflow), `merge_*` przez `DockerOperator` z obrazu `portfolio-spark:latest`
  (Java/Spark/Iceberg tylko w obrazie Sparka); montowanie `scripts/` ze ścieżki
  hosta (`HOST_PROJECT_DIR`), klucze AWS w `private_environment`,
  `mount_tmp_dir=False` (Docker Desktop), `auto_remove="success"`.
- Okna z szablonów Jinja: `extract_postgres` z 10-minutową zakładką,
  `extract_github` bez (problem `now()` dotyczy tylko Postgresa).
- `extract_github` bez zależności w dół: `validate.py` i `merge_*` obsługują
  tylko tabele Postgresa, więc podpięcie pod walidację sugerowałoby
  nieistniejącą zależność.
- `@daily`, `start_date` 1.09 (pierwotnie 18.10, zmienione po teście, patrz
  niżej), `catchup=False`, `max_active_runs=1` (bez nakładających się MERGE na tych
  samych tabelach Icebergu), `default_args.depends_on_past=True` (dzień N
  czeka na sukces lub pominięcie dnia N-1; brak cichych dziur).
- **Bramka jakości:** `validate.py` po sprawdzeniu wszystkich tabel kończy się
  kodem 1, jeśli cokolwiek trafiło do kwarantanny, więc task pada i `merge_*`
  się nie uruchamia. Bucket z env var (decyzja 8).
- **Pierwszy task TaskFlow:** `@task.short_circuit has_new_rows` czyta z XCom
  ostatnią linię `extract.py` („Done. N rows…”, `BashOperator` sam ją
  zapisuje); 0 wierszy = wszystko dalej oznaczone jako skipped (bez JVM, po
  dodaniu dbt także bez kredytów Snowflake). Gdy linii nie da się odczytać,
  nie pomija (bezpieczniej uruchomić niż zgubić dane).
- **Znane słabości walidacji (poprawka po 27.10):** `validate.py` sprawdza
  najnowszy plik tabeli, a nie pliki bieżącego runu (poprawka: ścieżki z
  extract przez XCom), a `merge_*` czytają cały raw razem z wierszami
  odrzuconymi (poprawka: pomijanie kwarantanny). Bramka zatrzymuje zły run,
  ale nie usuwa złych wierszy z późniejszych merge'y.

**Pytania z przeglądu DAG-a i czego uczą** (teoria: cheat sheet 31.8–31.12, 32):
- **Dlaczego extract przez `BashOperator`, a merge przez `DockerOperator`?**
  Operator wybiera się po tym, gdzie kod może działać: extract i walidacja
  potrzebują tylko bibliotek z obrazu Airflow, merge potrzebuje Javy/Sparka z
  obrazu Sparka, więc dostaje własny chwilowy kontener. Skrypty nie importują
  Airflow, więc działają też bez niego.
- **Co znaczą `SCRIPTS_DIR`, `HOST_PROJECT_DIR`, `SPARK_IMAGE`?** Pierwsza to
  ścieżka do skryptów WEWNĄTRZ kontenera Airflow (montowanie z compose). Druga to
  ścieżka repo na Twoim MACU: `DockerOperator` prosi Dockera na Macu o nowy
  kontener, a ten rozumie tylko ścieżki hosta. Trzecia to nazwa obrazu Sparka.
- **Dlaczego `{{ data_interval_start }}` w klamrach?** To szablon Jinja. Scheduler
  czyta plik DAG-a co ~30 s, kiedy żaden run jeszcze nie istnieje; wartości
  runu wstawia dopiero przy wykonaniu taska. Dlatego też kod na najwyższym
  poziomie pliku musi być lekki.
- **Po co `max_active_runs=1` i `depends_on_past=True`?** Pierwsze = jeden run
  naraz (bez równoległych MERGE na tych samych tabelach Icebergu i kilku JVM na
  laptopie). Drugie = dzień N czeka na sukces dnia N-1, więc błąd zatrzymuje
  kolejne dni zamiast zostawić cichą dziurę w danych.
- **Dlaczego `start_date` 1.09 i czemu test z 13.09 nic nie zrobił?** Data runu
  przed `start_date` daje run bez tasków (oznaczony jako sukces), a dane seed
  kończą się 15.09. Plus pułapka: run ręczny dla daty D przetwarza dzień D-1.
- **Dlaczego `extract_github` nie ma nic po sobie?** Dane z GitHuba lądują tylko
  w raw (walidacja i merge obsługują tabele Postgresa), a podpięcie go przed
  walidacją oznaczałoby, że awaria API GitHuba blokuje cały pipeline.
- **TaskFlow, TaskGroup, XCom — co jest czym, i czy przenieść skrypty na
  `@task`?** TaskFlow to sposób pisania taska (`@task` na funkcji), TaskGroup to
  folder w UI, XCom to mały schowek w bazie Airflow na wartości między taskami.
  Skrypty zostają skryptami: Spark i tak musi iść w swoim kontenerze, dane płyną
  przez S3, a `@task` zostaje do decyzji orkiestracji (decyzja 9).
- **Kiedy `has_new_rows` zwróci `False` i czemu „nie umiem odczytać” daje
  `True`?** `False` tylko przy 0 wierszy (wtedy reszta jest skipped, nie
  failed). Gdy linii nie da się odczytać, przepuszcza: niepotrzebny run kosztuje
  minutę Sparka, a niepotrzebne pominięcie to ciche zgubienie danych
  (fail-safe).
- **`pendulum` czy `datetime`, po co funkcja pomocnicza dla `DockerOperator`, co
  to `GITHUB_REPO`?** Oba typy dat działają, liczy się strefa (`pendulum`, bo
  Airflow używa go wewnątrz). Funkcja, bo oba taski Sparka dzielą 6 z 8
  parametrów. `GITHUB_REPO` to publiczne repo, z którego pobieramy dane przez
  API, ustawiane zmienną środowiskową z wartością domyślną.
- **Jak czytać komendy testowe (`2>&1 | grep`, `;`, `airflow dags test ...`)?**
  Logi Airflow idą strumieniem błędów, a `|` przekazuje tylko zwykłe wyjście,
  więc `2>&1` łączy oba przed filtrem; `;` uruchamia następną komendę zawsze,
  `&&` tylko po sukcesie. CLI ma budowę program → grupa → komenda → argumenty,
  jak `git` czy `docker compose`.
- **Merge commit, squash czy rebase przy PR?** Zostajemy przy squash: jeden PR =
  jeden commit na `main`, łatwy do cofnięcia (cheat sheet 12.3).

**Test 21.10, ścieżka bez danych:** `airflow dags list-import-errors` → brak
błędów. `airflow dags test ecommerce_pipeline 2026-10-20`: `extract_postgres`
0 wierszy, `extract_github` 580 (to samo okno dzień wcześniej dało 589, bo część
PR-ów zmieniono ponownie i przeszły do kolejnego okna; ograniczenie z
ADR-006), `has_new_rows` → `False`, `validate_raw` i `merge_*` pominięte.

**Dwie pułapki z testu 21.10:**
- `dags test` z datą 13.09 utworzył run **bez żadnego taska** i oznaczył go
  jako sukces: data logiczna była przed `start_date` (18.10). Mój błąd
  projektowy: `start_date` dobrany pod backfill 18–20.10, a dane w
  `postgres-source` kończą się na 15.09 (seed z przeszłymi datami; najwięcej
  zmian 11–13.09). Poprawka: `start_date` = 1.09, backfill w 6.6 na 11–13.09.
- **Okno runu ręcznego jest inne niż runu z harmonogramu.** Harmonogram i
  `backfill`: data logiczna D = okno `[D, D+1)`. Run ręczny (`dags test`,
  Trigger w UI): okno `[D-1, D)`, czyli ostatnie zakończone przed D (w logu:
  `data_interval_start=2026-09-12, data_interval_end=2026-09-13` dla D=13.09).
  Test „za 20.10” przetwarzał więc w praktyce 19.10. Żeby przetestować 13.09
  ręcznie, podaje się 14.09.

**Test 21.10, pełny run (okno 13.09):** `airflow dags test
ecommerce_pipeline 2026-09-14` → `data_interval` 13.09–14.09.
`extract_postgres` 26 wierszy (customers 1, products 1, orders 4, order_items
10, payments 4, order_status_history 7), `has_new_rows` → `True`,
`validate_raw` 26/26 poprawnych (bramka przepuściła), `merge_dimensions` i
`merge_facts` w kontenerach Sparka przez `DockerOperator` (~40 s każdy). Iceberg
po MERGE: customer 51, product 30, orders 200, order_items 502, payments 160,
order_status_history 493, **identycznie jak przed runem**: wiersze z 13.09 już
były w Icebergu, MERGE ich nie zdublował (dowód idempotentności). DAG działa
end-to-end Postgres → S3 → walidacja → Iceberg. Logi zdublowane (log do pliku i
terminala w `dags test`; `DockerOperator` przekazuje logi kontenera dwa razy).

**Git 22.10:** PR #13 zmergowany (DAG). Commit z dokumentacją trafił na
gałąź już zmergowanego PR (push po merge'u nie tworzy ani nie aktualizuje
zamkniętego PR), więc przeniesiony `git cherry-pick` na nową gałąź od `main` →
PR #15. Na przyszłość: dorzucać do PR przed kliknięciem merge.

**Zrobione (6.4b, 22.10): task dbt w DAG-u.**
- Kontener łączy się ze Snowflake'iem bez żadnego sekretu w repo: szablon
  `dbt_portfolio/profiles.yml.example` (same `env_var()`) zamontowany jako
  `profiles.yml`, klucz RSA zamontowany tylko do odczytu ze ścieżki hosta w
  `.env`; zmienne `DBT_SNOWFLAKE_*` przeniesione do `.env` (wcześniej były tylko w
  sesji terminala, niewidoczne dla `docker compose` z nowego terminala).
- Profil ma teraz targety `dev` (`dbt_dev_*`) i `prod` (`dbt_prod_*`);
  `PIPELINE_ENV` (domyślnie `dev`) wybiera target. TaskGroup `ingest`,
  `spark_merge`, `dbt`.
- `dbt debug` z kontenera: połączenie OK; `git [ERROR]` nieistotny (git
  potrzebny tylko do pakietów z repozytoriów git; `dbt_utils` jest z dbt Hub i
  już zainstalowany).

**Incydent 22.10: Snowflake od 7.10 nie widział zmian w 5 tabelach.**
- Objaw: pierwszy run z dbt → 7 testów `unique` na stagingu padło
  (`orders`, `order_items`, `payments`, `customers`, `products`), dbt pominął
  modele zależne (`SKIP=22`, `fct_orders` nieprzebudowana), task i run
  failed. Spark w tym samym czasie: 200/502/160/493, bez duplikatów.
- Diagnoza krok po kroku: (1) brak plików usunięć w Icebergu (tylko pliki
  danych, copy-on-write), więc nie merge-on-read; (2) Snowflake: „parquet file
  … `fact_orders/` … inaccessible”, czyli stara ścieżka sprzed §25, usunięta z
  S3; (3) Glue wskazuje metadane `00005` (22.10), Snowflake `00003` (7.10);
  (4) `SYSTEM$AUTO_REFRESH_STATUS`: `GENERALIZED_PIPE_STOPPED` od 7.10 11:38 dla 5
  tabel, `RUNNING` tylko dla `order_status_history` (MERGE tylko dopisuje);
  przyczyna „invalid file … was found in the Iceberg metadata snapshot”.
- Mechanizm: `rewrite_table_path` zachował identyfikatory snapshotów, a
  zmienił ścieżki; Snowflake odświeża przyrostowo po snapshotach i odrzucił
  plik z nową ścieżką w „znanym” snapshocie. Weryfikacja z §25 objęła Glue i
  Sparka, nie Snowflake'a.
- Naprawa: ręczne `REFRESH` zwracało ten sam błąd, więc odtworzenie
  `portfolio_ecommerce_db` poleceniem z `GET_DDL` (po inwentaryzacji grantów i
  polityk), potem polityki na `customer`, potem granty (analityk nie miał
  dostępu bez polityk). Pierwsze uruchomienie bloku z politykami za wcześnie
  (tabele nie były jeszcze wykryte, `sync_interval_seconds = 30`); błąd
  zatrzymał też granty, więc bez okna bez polityk.
- Weryfikacja: Snowflake 51/30/200(200 unikalnych)/502/160/493, metadane
  `00005`, odświeżanie `RUNNING`; analityk: 2 wiersze, oba DE, e-mail
  `***@example.org`; ponowny `airflow dags test ecommerce_pipeline 2026-09-14`:
  wszystkie taski SUCCESS, **dbt `PASS=51 ERROR=0`**.
- Lekcje: (1) po fizycznym przeniesieniu tabeli sprawdzać każdy silnik, nie
  tylko katalog, i to przed usunięciem starych plików (ADR-005, aktualizacja
  2026-10-22; `CLAUDE.md` decyzja 6); (2) ciche zatrzymanie odświeżania nie dało
  żadnego alertu przez dwa tygodnie, a wyłapały je dopiero testy dbt: kandydat
  do monitoringu w fazie 9 (np. codzienne sprawdzenie
  `SYSTEM$AUTO_REFRESH_STATUS` dla wszystkich tabel).

**Pytania z tego dnia i czego uczą** (teoria: cheat sheet 12.13, 25.17, 27.3,
31.13, 31.14, 32.4, 32.5):
- **Czym różni się `portfolio_ecommerce_db` (ikona ogniwa) od `portfolio_dbt`, i
  widok od tabeli?** Pierwsza to catalog-linked database: nic nie przechowuje,
  pokazuje Icebergi z Glue i odświeża ich metadane. Druga to zwykła baza, w
  której dbt buduje swoje obiekty. Widok to zapisane zapytanie liczone przy
  każdym odczycie (przepis), tabela to zapisany wynik (gotowe danie); dlatego
  widoki staging od razu pokazały duplikaty, gdy Snowflake czytał dwa snapshoty.
- **Co zatrzymało pipeline i czy `dags test` to próba na sucho?** dbt w ramach
  `build` pominął modele zależne od złego testu, a Airflow widzi tylko kod
  wyjścia taska, więc oznaczył task i run jako failed. `dags test` wykonuje
  wszystko naprawdę (S3, MERGE, Snowflake); bezpieczne tylko dzięki
  idempotencji.
- **Co robi test i czy coś zapisuje? Co to RLS i `int_`?** Test to SELECT
  szukający złych wierszy (0 = PASS), domyślnie nic nie zapisuje. RLS (row
  access policy) filtruje wiersze zależnie od roli, np. analityk widzi tylko
  DE. `int_` = intermediate, warstwa pośrednia między stagingiem a martem.
- **Skąd `PIPELINE_ENV` i jak zrobić dev/prod „jak w korporacji”?** Zmienna z
  `.env` → compose → kontener; DAG przekazuje ją jako `--target` do dbt
  (`dbt_dev_*` / `dbt_prod_*`). W korporacji kod jest jeden, a środowiska różnią
  się plikami konfiguracyjnymi i osobnymi wdrożeniami. Osobny bucket S3 dla
  prod odłożony do Terraformu (faza 8): prod Icebergu nie da się odbudować z
  raw, bo merge czytający wszystko naraz zwinąłby historię SCD2.
- **Dlaczego `git push` nie stworzył PR?** Push tylko wysyła commity; PR tworzy
  `gh pr create` albo przycisk w UI. Push na gałąź już zmergowanego PR nie trafia
  nigdzie, więc nowa gałąź od `main` i `git cherry-pick <hash>` (kopiuje jeden
  commit).
- **Po co sumy kontrolne md5 przy zapisie plików?** Suma to odcisk treści
  pliku: przed zapisem potwierdza, że nikt nie zmienił pliku, po zapisie, że w
  repo jest naprawdę nowa wersja (lekcja z 20.10).
- **Komendy `env`, `sed`, `printf >>`, `grep -l`, `diff`, `2>/dev/null`** —
  użyte do przeniesienia zmiennych `DBT_SNOWFLAKE_*` do `.env` bez wyświetlania
  wartości (cheat sheet 32.4).

**Zrobione (22.10): testy dbt rozszerzone** (inspiracja: post z LinkedIn o
testach dbt; cheat sheet 25.18–25.19).
- Staging: `relationships` ×5 (każdy klucz obcy ma rodzica),
  `accepted_values` ×4 (statusy zamówień, metody i statusy płatności),
  `dbt_utils.expression_is_true` ×4 (`valid_to > valid_from` w SCD2,
  `quantity > 0 AND unit_price >= 0`, `amount >= 0`), freshness na źródłach
  `orders` i `payments` (`warn_after` 2 dni, bez `error_after`).
- `fct_orders`: kwoty nieujemne (error) i „zapłacone + zwrócone ≤ wartość
  zamówienia” jako `severity: warn` (klient po zwrocie może zapłacić ponownie);
  `store_failures: true` przy regułach biznesowych.
- Unit testy: `int_customer_versions` (okna obowiązywania wersji) i
  `fct_orders` (statusy płatności, zamówienie bez pozycji → 0, kraj w chwili
  zamówienia LY → FR, flaga `is_before_first_customer_version`).
- DAG: `dbt.dbt_source_freshness` przed `dbt_build` (tylko ostrzega); w `prod`
  `dbt build --exclude-resource-type unit_test` (słownik `DBT_BUILD_FLAGS`,
  do przeniesienia do `config/<env>.yaml`). Cały przepływ w jednej linii
  zależności.
- Wynik: `dbt build --target dev` → **`PASS=68 ERROR=0`**, po dodaniu unit
  testu `fct_orders` `--select fct_orders` → `PASS=18`; freshness `WARN` dla obu
  źródeł (dane kończą się 15.09, oczekiwane); `list-import-errors` pusto.
- Moja pomyłka: zakładałem, że reguła „≤ wartość zamówienia” da `WARN` na
  danych seed; przeszła, bo seed generuje płatności zgodne z wartością
  zamówień. Komentarz w `schema.yml` poprawiony.
- Znane ryzyko (do 6.5): dbt startuje zaraz po `merge_facts`, a Snowflake
  odświeża metadane Iceberga co 30 s, więc może przeczytać poprzedni snapshot.
  Do rozważenia sensor na wersję metadanych albo `REFRESH` przed dbt.
- Porządek w cheat sheecie: zdublowany numer 25.16 → „Bazy w Snowflake” to
  teraz 25.17.

**Pytania o testy i DAG i czego uczą** (teoria: cheat sheet 25.18, 25.19, 31.15, 32.6):
- **Dlaczego zależności w jednej linii i jak czytać DAG?** `a >> b` = b startuje
  po sukcesie a; czyta się od lewej, a cały przepływ w jednym miejscu jest
  czytelniejszy niż zależności rozrzucone po pliku. Taski bez nikogo przed sobą
  (korzenie) startują równolegle.
- **Po co TaskGroup, skoro `extract_github` nic nie blokuje?** TaskGroup to
  folder w UI grupujący taski według etapu (ingest / spark_merge / dbt), a nie
  zależność; `extract_github` jest w ingest, bo to etap pobierania.
- **Dlaczego testy w dbt, a nie w Airflow, i czy są częścią DAG-a?** dbt zna
  modele i ich zależności, test leży obok modelu, a `build` pomija tylko to, co
  zależy od złego modelu; Airflow widzi tylko kod wyjścia. Testy są w DAG-u
  przez task `dbt_build`; uruchomienia z terminala to sprawdzanie w trakcie
  pisania.
- **Dlaczego dane w unit teście są wpisane na sztywno, czy to tylko SCD2, dev
  czy prod?** Unit test sprawdza logikę SQL na ręcznie dobranych przypadkach
  brzegowych (jak pytest), więc dane zmieniają się tylko razem z logiką. Dotyczy
  jednego modelu (u nas SCD2 i `fct_orders`) i działa w dev/CI. Testy danych
  sprawdzają prawdziwe dane, więc działają wszędzie, także w prod.
- **Dlaczego „18” przy jednej tabeli i dlaczego komenda jest taka długa?** Jedna
  komenda, a `--select fct_orders` bierze model z jego testami: budowa, unit
  test i 16 testów danych. Długość komendy: `docker exec` (dbt i klucz są w
  kontenerze), `cd` + pełna ścieżka do dbt (osobny venv), `grep` do filtrowania
  logu. Na co dzień: `docker exec -w <katalog> ...`, a w korporacji dbt lokalnie
  z venv albo przez Makefile.

**Pytania o CI i stan projektu** (teoria: cheat sheet 31.16):
- **Jakie „ręczne testy” i dlaczego PR nie odpala `dbt build`?** Ręczne testy to
  komendy, które uruchamiasz i wklejasz (`dbt build`, `list-import-errors`, `dags
  test`). Repo nie ma CI: GitHub bez `.github/workflows/` niczego nie uruchamia.
  W korporacji każdy PR automatycznie przechodzi lint, test importu DAG-ów i
  `dbt build` zmienionych modeli w schemacie CI, a merge jest zablokowany do
  zielonych checków. U nas to faza 7.
- **Ile jeszcze zostało?** Stan 22.10: fazy 0–5 gotowe, faza 6 ok. 70%; dalej 7
  CI/CD, 8 Terraform, 9 observability. Po 27.10 bez Snowflake'a, więc fazy 7–9
  na AWS/Spark/Airflow.

**Zrobione (22.10): dev i prod jako konfiguracja, dwa wdrożenia.**
- `config/dev.yaml` i `config/prod.yaml`: tylko to, co się różni (harmonogram:
  dev 00:00, prod 03:00 UTC, żeby dwa MERGE do tych samych tabel Icebergu nie szły
  naraz; prod `--exclude-resource-type unit_test`). Target dbt = sam
  `PIPELINE_ENV`. DAG przy
  wczytaniu czyta `config/<PIPELINE_ENV>.yaml`; brak pliku = błąd importu, a nie
  ciche wartości domyślne. W kodzie nie ma żadnego `if prod`.
- `docker-compose.yml`: `./config` zamontowany tylko do odczytu w Airflow;
  `PIPELINE_ENV: dev` wpisany na stałe (już nie z `.env`); drugi Airflow w
  profilu `prod` (własna baza metadanych, init, UI na 8081 oznaczone „PROD”,
  scheduler, logi w `logs/prod`), startowany tylko przez `docker compose
  --profile prod up -d`.
- Test: dev `No data found`, harmonogram „At 00:00”; prod `No data found`,
  „At 03:00” — ten sam plik DAG-a, inne zachowanie wyłącznie z konfiguracji.
  DAG w prodzie wstrzymany (pauza) do czasu pierwszego runu.
- Pułapka (23.10): po edycji plików w `config/` oba schedulery zgłosiły
  `FileNotFoundError: .../pipeline_config/dev.yaml`, choć pliki były na Macu.
  `ls` w kontenerze pokazał pusty folder (`total 0`): Docker Desktop zgubił
  widok zamontowanego katalogu. Naprawa: `docker compose --profile prod up -d
  --force-recreate <serwisy Airflow>`; bazy nietknięte. Dobry znak: brak
  configu dał głośny błąd importu, nie ciche wartości domyślne. Zasada: po
  zmianie zamontowanego configu sprawdzić import, a przy błędzie odtworzyć
  kontenery (w firmie zmiana configu i tak oznacza nowe wdrożenie).
- Do poprawy: compose zbudował osobny obraz dla każdego serwisu (identyczne,
  z cache'u). W firmie jeden obraz z jedną wersją idzie na wszystkie
  środowiska; do zrobienia przez wspólne `image:` w serwisach Airflow.

**Pytania o dev/prod i czego uczą** (teoria: cheat sheet 31.17):
- **Na co się zgodziłem w kolejnych zmianach i co zrobił `docker compose up
  -d`?** Trzy małe kroki zamiast jednej dużej komendy (wcześniejszą, ze 100
  liniami konfiguracji naraz, odrzuciłeś, bo nie dało się jej przejrzeć). `up -d`
  utworzył od nowa tylko kontenery, którym zmieniły się montowania (`Started`),
  resztę zostawił (`Running`): montowania ustawia się wyłącznie przy tworzeniu
  kontenera.
- **Jak prod nadpisze `PIPELINE_ENV: dev`?** Wspólne zmienne są w compose raz,
  pod kotwicą YAML `&airflow-common-env`. Prod wkleja je (`<<:
  *airflow-common-env`) i dopisuje swoje; przy powtórzonym kluczu wygrywa
  późniejszy. Zmieniają się tylko `PIPELINE_ENV` i adres bazy metadanych.
- **Skąd biorą się zmienne środowiskowe?** Droga zawsze ta sama: źródło
  (`.env` dla sekretów, stała w compose, `${PWD}` z terminala) → sekcja
  `environment:` w compose → env procesu w kontenerze → czytelnik (sam Airflow,
  skrypty przez `os.environ`/`boto3`, dbt przez `env_var()`, DAG, który klucze
  AWS przekazuje dalej do kontenera Sparka). Pełna tabela w cheat sheecie.
- **Jak `PIPELINE_ENV` zmienia się z dev na prod?** Nie zmienia się nigdy. Są
  dwa komplety kontenerów, każdy z wartością wpisaną na stałe od startu; ten sam
  DAG robi co innego, bo czyta inny plik konfiguracyjny. „Przejście na prod” to
  uruchomienie drugiego kompletu, a w firmie: ta sama wersja kodu wdrożona przez
  CI na osobny Airflow.
- **Dlaczego wyszukiwarka nie znalazła `PIPELINE_ENV=prod`?** Przed dopisaniem
  proda tej wartości nie było, a składnia zależy od miejsca: w compose (YAML)
  `PIPELINE_ENV: prod`, w `.env` i w działającym kontenerze `PIPELINE_ENV=...`;
  `.env` jest w `.gitignore`, więc VS Code go nie przeszukuje.
- **Czy da się zrobić to jak w firmie?** Bliżej: jeden szablon compose dla
  Airflowa i plik wartości per środowisko (`env/prod.env`), każde wdrożenie jako
  osobny projekt (`docker compose -p portfolio-prod --env-file ...`), co wymaga
  wydzielenia Postgresa-źródła i Sparka do osobnego pliku (kilka godzin). W
  pełni: dwa Airflowy w chmurze na osobnych kontach (faza 8). Wybrany wariant z
  profilem, bo trial kończy się 27.10.
- **Po co osobno `PIPELINE_ENV` i `DBT_TARGET`?** Pierwsza wersja czytała
  target dbt z configu (`dbt.target`), „na wypadek”, gdyby nazwy środowisk i
  targetów się rozjechały (np. osobne środowisko CI piszące do targetu `dev`,
  target per developer). Tu oba mają zawsze te same wartości, więc to była
  zbędna komplikacja: wycofana, `--target {PIPELINE_ENV}`. Zasada z ADR-ów:
  nie dodawać złożoności bez realnej potrzeby. Przy okazji: `target: dev` w
  `profiles.yml` to tylko target domyślny (gdy komenda nie ma `--target`), a
  `outputs` to lista dostępnych targetów.
- **Czy dev i prod mają osobny Snowflake / Databricks?** Snowflake: zwykle jedno
  konto, środowiska rozdzielone bazami, rolami i warehouse'ami (osobne konta
  głównie przy ostrych regulacjach). Databricks: standardem są osobne
  workspace'y dla dev i prod w jednym koncie, plus katalogi per środowisko w
  Unity Catalog. U nas jedna baza `PORTFOLIO_DBT`, schematy `dbt_dev_*` i
  `dbt_prod_*`.
- **Czy to znaczy, że nie udostępniłem folderu kontenerowi?** Nie: montowanie
  jest w compose, Docker Desktop domyślnie udostępnia cały `/Users`, a o 22:58
  DAG czytał `dev.yaml` bez błędu. Zawiódł widok w trakcie działania: pliki
  zmieniłem z innej warstwy niż VS Code i Docker Desktop tego nie zauważył.
  Rozpoznanie: `ls` w kontenerze pokazuje co innego niż na Macu.
- **Co robi `--force-recreate`?** Usuwa wskazane kontenery i tworzy nowe z tego
  samego obrazu i konfiguracji, nawet gdy nic się nie zmieniło (zwykłe `up -d`
  robi to tylko przy zmianie). Przetrwa wszystko poza kontenerem (wolumeny z
  bazami, zamontowane foldery); znika tylko to, co zapisano wewnątrz. Firma:
  restart wdrożenia (`kubectl rollout restart`), kontenery są bezstanowe.
- **Czy mam osobny scheduler dla dev i prod?** Tak, każde środowisko ma swój
  komplet: baza metadanych, init, webserver, scheduler (dev bez dopisku, bo
  był pierwszy; prod z `-prod`). Wspólne są `postgres-source` i
  `spark-iceberg`. W firmie nazwy się nie mieszają, bo to osobne klastry.
- **Co to Helm?** Menedżer wdrożeń dla Kubernetesa: chart (szablon całego
  Airflowa) + `values-dev.yaml`/`values-prod.yaml`; CI robi `helm upgrade` po
  merge'u. Ten sam pomysł co nasz blok compose + nadpisania proda.

**Pytania o jakość danych i część Advanced planu (23.10):**
- **Gdzie jest Great Expectations?** W planie w części Advanced (pozycja 13),
  jako nadbudowa nad `validate.py` w warstwie raw; w fazie 2 świadomie wybrana
  lekka własna walidacja. W firmach GE jest dziś rzadszy: gdzie jest dbt,
  zwykle wystarczają testy dbt (+ `dbt-expectations`, Elementary), Soda jako
  reguły w YAML-u, GE głównie w pipeline'ach Python/Spark przed warehouse'em.
- **Co jeszcze jest w Advanced?** 14 Elementary/observability (pokrywa się z
  fazą 9), 15 porównanie z Databricks w `/experiments/databricks` (Free Edition
  nie wygasa), 16 rozbudowane governance (częściowo: RLS + maski na `customer`
  i `fct_orders`; dalej tylko do 27.10).
- **GE czy testy dbt, czy kilka naraz?** Decyzja: kilka narzędzi na tych samych
  regułach (klucz, dozwolone statusy, kwota ≥ 0, wolumen), żeby różnice było
  widać wprost: własny `validate.py` (punkt odniesienia), GE i Soda Core na raw
  (po 27.10, zamiast samej poprawki `validate.py`), 2–3 testy `dbt-expectations`
  w Snowflake (przed 27.10, w 6.7). Porównanie w ADR, jak dlt.
- Pytania kontrolne z planu projektu („co, jeśli pipeline uruchomi się dwa
  razy?”, „jak zrobić backfill 3 miesięcy?”, „jak wykryć ciche uszkodzenie
  danych?”) posłużą za szkielet README w 6.7.

**Git 23.10:** PR #18 zmergowany za wcześnie (bez usunięcia `target`), drugi
commit poszedł na gałąź po merge'u (ta sama lekcja co 22.10) → PR #19; dokumentacja
→ PR #20. Przy #20 konflikt w `PROGRESS_LOG.md`: ten sam fragment zmieniony różnie
na gałęzi i na `main`, git nie zgaduje, którą wersję wziąć. Rozwiązany w UI
(„Accept current change”: po stronie `main` fragment był pusty, więc nic nie
zginęło), kontrola `grep -c "<<<<<<<" PROGRESS_LOG.md` → 0.

**Pytania o Asanę i czego uczą** (teoria: cheat sheet 31.18):
- **Czy da się wykorzystać Asanę w projekcie?** Tak, w dwóch miejscach. (1) W
  6.5 jako cel alertów: `on_failure_callback` zakłada zadanie w projekcie „Data
  incidents” przez API Asany. (2) Po 27.10 jako trzecie źródło danych (zadania,
  projekty): filtr `modified_since` i API zdarzeń z tokenem synchronizacji to
  inny wzorzec przyrostowego pobierania niż Postgres i GitHub; dlt ma gotowe
  źródło Asany, więc to naturalny kandydat do porównania w ADR-007 (GitHub
  własnym skryptem, Asana przez dlt).
- **Czy alerty pojawią się w Asanie od razu?** Tak, po ostatecznym błędzie taska
  (po wszystkich retry) callback wysyła jedno zapytanie `POST /tasks` i zadanie
  jest w Asanie po kilku sekundach. To przepływ jednostronny (Airflow → Asana),
  nie synchronizacja. Pierwsza wersja nie zamyka zadania po udanym runie i nie
  łączy powtórek; kolejne kroki: `on_success_callback` zamykający otwarte
  zadanie i sprawdzenie „czy otwarte zadanie dla tego taska już istnieje”
  (wtedy komentarz zamiast nowego zadania). W firmie: Slack/PagerDuty do
  powiadomień, ticket (Jira/Asana) dla incydentów do rozwiązania.
- Do zrobienia przez Ciebie: darmowe konto Asana (API jest w planie Personal),
  projekt „Data incidents”, Personal Access Token; w `.env` `ASANA_TOKEN` i
  `ASANA_PROJECT_ID` (liczba z adresu projektu). Token nigdy w repo.

**Zrobione (6.5, 23.10): retry z rosnącą przerwą, `execution_timeout`, alert do Asany.**
- `config/dev.yaml` i `prod.yaml`: blok `retries` (dev: 2 ponowienia, przerwa od
  30 s, limit 5 min; prod: 3, od 2 min, limit 30 min, bo o 03:00 nikt nie
  patrzy) i `execution_timeout_minutes` per etap (extract 10, validate 10,
  spark_merge 15, dbt 20; ok. 5–10× normalnego czasu; w obu środowiskach te
  same, bo dane są wspólne, w firmie decydowałby wolumen proda).
- DAG: `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`
  i `on_failure_callback` w `default_args` (obejmują każdy task),
  `execution_timeout` per task. `validate_raw` i `has_new_rows` mają
  `retries=0`: ich porażka jest deterministyczna (kwarantanna, odczyt XCom),
  ponowienie dałoby ten sam wynik. Retry jest bezpieczny, bo każdy task jest
  idempotentny.
- `dags/alerting/asana.py`: callback woła się raz, po OSTATNIEJ nieudanej próbie
  (także po timeoucie), i wysyła jedno `POST /tasks` do projektu „Data
  incidents”: tytuł `[env] dag.task failed (window …)`, w notatce run, okno,
  link do logu i błąd. Nigdy nie rzuca wyjątku (zasłoniłby oryginalny błąd w
  logu); brak tokenu = głośny wpis w logu, bez awarii taska.
  `dags/.airflowignore`: scheduler nie parsuje modułu jako DAG-a.
- Sekrety: `ASANA_TOKEN` (PAT z prywatnego konta) i `ASANA_PROJECT_ID` (liczba po
  `/project/` w adresie) w `.env`, przekazane przez compose do obu Airflowów.

**Test na żywo 23.10 (dev, run z harmonogramu, okno 22.10):**
`docker compose stop postgres-source` jako realistyczny błąd przejściowy (baza
źródłowa nie odpowiada), bez zmiany kodu.

| Próba | Start (UTC) | Przerwa | Wynik |
|---|---|---|---|
| 1 | 18:40:10 | — | `UP_FOR_RETRY` |
| 2 | 18:40:43 | 33 s (oczekiwane 30–60) | `UP_FOR_RETRY` |
| 3 | 18:42:32 | 109 s (oczekiwane 60–120) | `FAILED` → zadanie w Asanie po 1 s |

Jedno zadanie w Asanie (nie trzy), trafiło do pierwszej sekcji projektu;
`extract_github` sukces mimo awarii Postgresa, reszta `upstream_failed` (bez
Sparka i dbt). Okno w logu `2026-10-21T23:50` → `2026-10-23T00:00` (dzień 22.10
z zakładką). Druga połowa: `docker compose start postgres-source` + `airflow
tasks clear ... -t '^ingest\.extract_postgres$' -d -s 2026-10-22 -e 2026-10-22
-y` → run zielony, `has_new_rows` = False (0 wierszy w oknie 22.10, seed kończy
się 15.09), reszta `skipped`. Zadanie w Asanie zamknięte ręcznie (pierwsza
wersja działa tylko w jedną stronę).

**Odkryte przy teście: zator `depends_on_past` po TaskGroupach.**
- Objaw: run `scheduled__2026-10-22` od 18:27 w stanie `running`, wszystkie
  taski bez statusu, przez `max_active_runs=1` blokował kolejne runy. `airflow
  tasks failed-deps`: „depends_on_past is true …, but the previous task instance
  has not run yet”.
- Przyczyna: TaskGroup zmienia `task_id` (`extract_postgres` →
  `ingest.extract_postgres`). Przy `catchup=False` „poprzedni run” to
  `manual__2026-10-20` (`dags test` z 21.10, sprzed TaskGroup), który nie ma
  instancji o nowym ID, więc Airflow czekał bez końca. Na Gridzie widać to w
  runie 13.09: zielone `has_new_rows`/`validate_raw` (spoza grup), puste `extract_*`.
- Naprawa: usunięcie z metadanych runu testowego `manual__2026-10-20` (Browse →
  DAG Runs → Delete; nie `airflow dags delete`, które kasuje całą historię DAG-a).
  Dane w S3/Iceberg/Snowflake i pliki logów nietknięte. Poprzednim runem stał się
  `manual__2026-09-14` (powtórzony 22.10 z nowymi ID), scheduler od razu
  wystartował task.
- Lekcje: (1) zmiana `task_id`, także przez TaskGroup, zrywa historię taska
  (`depends_on_past`, czasy trwania); w firmie: nie zmieniać ID w działającym
  DAG-u (`prefix_group_id=False`) albo przy wdrożeniu świadomie przeciąć historię
  (jednorazowy run z „Ignore Task Deps”), historii runów na prodzie się nie
  kasuje (audyt). (2) Zator to cisza, nie porażka, więc alert do Asany go nie
  łapie; kandydat: `dagrun_timeout` + callback na poziomie DAG-a.
- Moje błędy: komendy wypisane po przecinku w zdaniu (wklejone do terminala nie
  zadziałały; od teraz komendy tylko w bloku kodu, w formie do wklejenia) i data
  z UI (23.10 = koniec okna) zamiast daty logicznej (22.10) w `failed-deps`.

**Git 23.10:** `git push origin --delete` dla `feat/dbt-tests`,
`feat/dev-prod-config`, `feat/airflow-dbt-task` → „remote ref does not exist”:
na GitHubie już ich nie było, zostały tylko lokalne kopie `origin/*`, które
sprząta dopiero `git fetch --prune`. Lokalne `feat/dbt-tests` usunięte (sprawdzone
wcześniej, że nic z niego nie brakuje na `main`).

**Pytania z tego kroku i czego uczą:**
- **Czy konto w Asanie zakładać na sztuczny mail (np. `PG_ASANA@PG.com`)?** Nie:
  Asana wymaga potwierdzenia maila, a domena prawdziwej firmy może przypiąć konto
  do jej organizacji w Asanie i wygląda na podszywanie się. Do portfolio
  wystarcza PAT z prywatnego konta; osobna tożsamość „bota” = alias Gmaila
  (`mail+asana-bot@gmail.com`).
- **Jak to się robi w firmie?** Konto techniczne zakłada IT/admin na wniosek (ticket,
  właściciel, akceptacja, nazwa typu `svc-data-alerts`). Dwie opcje: service
  account Asany (plany Enterprise, tworzy super admin, tylko API, ale z pełnymi
  uprawnieniami widzi wszystkie dane organizacji, więc rzadko dla wąskiej
  integracji) albo częściej użytkownik integracyjny: prawdziwa skrzynka funkcyjna
  w domenie firmy, licencja, zaproszenie tylko do jednego projektu, PAT z jego
  konta (najmniejsze uprawnienia). Token w vaulcie (Key Vault / Secrets Manager /
  HashiCorp Vault), Airflow czyta go jako Connection przez secrets backend; rotacja
  i przeglądy dostępu. Często zamiast Asany: alert na Slack/Teams/PagerDuty,
  ticket w Jirze/ServiceNow.
- **`,` vs `&&` vs `;`?** Przecinek nie jest składnią shella (git dostał całą linię
  jako swoje argumenty). `&&` = następna komenda tylko po sukcesie poprzedniej,
  `;` = zawsze. W firmie jednorazowe kroki wpisuje się po kolei, `&&` jest w
  skryptach/Makefile/CI.
- **Co zrobiły komendy gita i co naprawiło stare gałęzie?** Gałąź żyje w trzech
  miejscach: lokalnie (`feat/x`), jako lokalna kopia stanu GitHuba (`origin/feat/x`,
  aktualizuje ją tylko `fetch`/`pull`) i na GitHubie. Zwykły `fetch` niczego nie
  usuwa, `--prune` kasuje kopie gałęzi, których na GitHubie już nie ma. W firmie
  raz: `git config --global fetch.prune true` + w repo „Automatically delete head
  branches”.
- **Dlaczego zatrzymujemy `postgres-source`?** Retry i alert działają tylko przy
  porażce, więc trzeba ją wywołać celowo: najczęstszy realny powód retry (baza
  źródłowa chwilowo niedostępna), jedna odwracalna komenda, dane w wolumenie,
  Airflow ma osobną bazę metadanych, kod bez zmian. W firmie: test na dev/stagingu
  (złe hasło w connection, chaos testy), nigdy na współdzielonej bazie.
- **Dlaczego `tasks clear`, a nie uruchomienie całego DAG-a?** Naprawiamy ten
  konkretny run (okno 22.10), a nie dokładamy nowego: czerwony run zostałby w
  historii i przez `depends_on_past` zablokował kolejne dni. `clear` zeruje stan
  wybranych instancji (`-t` regex nazwy, `-d` z downstream, `-s/-e` które runy,
  `-y` bez potwierdzenia), scheduler uruchamia je ponownie (nowa próba =
  `attempt=4`), `extract_github` (sukces) nietknięty. Bezpieczne dzięki
  idempotencji. W firmie: przycisk „Clear” z „Downstream” w UI.
- **Dlaczego reszta `skipped`?** Okno 22.10 ma 0 wierszy, `has_new_rows` (sam
  task = success) zdecydował „nic do roboty” i short-circuit pominął resztę: bez
  JVM i kredytów Snowflake, run zielony.
- **Skąd dbt w Airflow i czy mam dwa dbt?** Airflow nie zawiera dbt; dołożyliśmy go
  sami w 6.1 do naszego obrazu (`airflow-docker/Dockerfile`, osobny venv
  `/opt/airflow/dbt_venv`), bo `BashOperator` przy `LocalExecutor` wykonuje komendę
  w kontenerze schedulera, więc program `dbt` musi tam być. Instalacje są dwie, w
  tych samych wersjach: `.venv` na Macu (praca z terminala, `~/.dbt/profiles.yml`) i
  venv w obrazie (DAG, profil z repo + `.env`); kod modeli wspólny. „dbt z Airflow”
  i „dbt z Dockera” to ta sama instalacja. W firmie: jeden obraz z CI używany
  wszędzie albo dbt w osobnym obrazie (`DockerOperator`/`KubernetesPodOperator`),
  albo Astronomer Cosmos (każdy model dbt jako osobny task Airflow).
- **Jak działa `DockerOperator` i czy Java/JAR-y go wymuszają?** Kod w schedulerze
  przez `docker.sock` prosi silnik Dockera na Macu o NOWY, chwilowy kontener z
  obrazu `portfolio-spark:latest` (montowanie `scripts/` ze ścieżki Maca, klucze
  AWS, komenda `python3 merge_*.py`), uruchomiony obok schedulera, nie w nim
  (Docker-out-of-Docker); operator przekazuje logi i czeka na kod wyjścia, po
  sukcesie kontener znika. Stały `spark-iceberg` to inny kontener z tego samego
  obrazu, DAG go nie używa. Java nie wymusza `DockerOperator`, to wybór: Java +
  Spark + JAR-y w obrazie Airflow = ciężki obraz, konflikty wersji, przebudowa
  Airflow przy każdej zmianie Sparka. W firmie Airflow w ogóle nie liczy, tylko
  zleca na klaster (`SparkSubmitOperator`, EMR, Glue, Databricks,
  `KubernetesPodOperator`), jak ADF odpalający job w Databricks.
- **Dlaczego zadanie w Asanie zamykam ręcznie?** Pierwsza wersja wysyła tylko `POST`
  przy porażce. Auto-zamknięcie wymaga `on_success_callback`, odnalezienia
  zadania (zapamiętane ID albo szukanie otwartego po nazwie), `PUT` z
  `completed: true` i warunku, żeby nie pytać Asany przy każdym sukcesie; plus
  deduplikacja (komentarz zamiast nowego zadania). W firmie alert (Slack,
  PagerDuty) zamyka się sam po ustąpieniu problemu, a ticket incydentu zamyka
  człowiek po ustaleniu przyczyny (czy nie ma dziury w danych, dlaczego baza padła).
- **Gdzie jest ID projektu w Asanie i jak zrobić PAT?** (przy podłączaniu
  alertu) ID to liczba po `/project/` w adresie projektu
  (`app.asana.com/1/<workspace>/project/<ID>/list/...`; pierwsza liczba to
  workspace, nie projekt); w aplikacji desktopowej bez paska adresu: strzałka
  przy nazwie projektu → „Copy project link”. PAT: awatar → Settings → Apps →
  View developer console → Personal access tokens → „+ Create new token”; token
  widać tylko raz. PAT działa jak hasło do całego konta, zadania powstają „jako
  Ty”; w firmie token konta technicznego z vaulta.
- **Dlaczego VS Code pokazuje moje zmiany „na main”, skoro nic nie
  commitowałem?** (po edycjach plików, przed utworzeniem gałęzi) Niezacommitowane
  zmiany są w working tree, nie na żadnej gałęzi; `git switch -c <gałąź>` zabiera
  je ze sobą, a na gałąź trafiają dopiero commitem. Dlatego gałąź wystarczy
  utworzyć przed commitem. Liczby przy plikach w VS Code (`8`, `1`) to
  ostrzeżenia Pylance: lokalny `.venv` nie ma Airflow, więc edytor nie rozpoznaje
  importów; prawdziwy test to `airflow dags list-import-errors` w kontenerze.
- **Czy przy teście awarii mam coś uruchamiać?** (po zatrzymaniu bazy) Nie:
  scheduler sam startuje task, sam robi retry i sam woła callback; obserwuje się
  Grid, logi kolejnych prób i Asanę.
- **Dlaczego UI pokazuje run „23.10”, a CLI chce 22.10?** (przy `failed-deps`)
  Grid w Airflow 2.9 pokazuje koniec okna (moment, od którego run mógł
  wystartować), a `execution_date` w `run_id` to data logiczna = początek okna.
  Komendy CLI biorą datę logiczną albo `run_id`; najpewniej podawać `run_id`
  (np. `scheduled__2026-10-22T00:00:00+00:00`) z `airflow dags list-runs`.
- **Po co dwa dbt (Mac i obraz Airflow), jak to jest w firmie i co to CI?**
  (dopytanie po wyjaśnieniu instalacji) Jeden kod modeli, jeden program
  zainstalowany w dwóch miejscach, w których ten kod musi działać: na Macu do
  szybkiej pracy (`dbt run --select`, rozszerzenie dbt w VS Code) i w obrazie
  Airflow, bo tam o 00:00 scheduler sam uruchamia `dbt build`. Jedna instalacja
  jest możliwa (praca przez `docker exec` albo devcontainer, czyli VS Code
  otwierający repo wewnątrz kontenera). CI (Continuous Integration) to automat
  (u nas byłyby to GitHub Actions, `.github/workflows/`), który przy każdym PR
  robi lint, test importu DAG-ów i `dbt build` zmienionych modeli w schemacie CI
  (merge zablokowany do zielonych checków), a po merge'u buduje obraz, nadaje mu
  wersję (hash commita) i wysyła do rejestru (ECR/ACR); CD (Continuous
  Deployment) wdraża TEN SAM obraz na dev, potem na prod. W firmie obraz buduje
  się raz, w CI, nigdy na laptopie, więc dev i prod uruchamiają bit w bit ten sam
  dbt; laptop: venv z lockfile'a sprawdzanego w CI albo devcontainer. U nas tego
  nie ma, bo nie ma jeszcze CI (faza 7) ani serwera i rejestru (dev i prod to
  dwa komplety kontenerów na Macu; budowa obrazu w CI ma sens dopiero z
  Airflowem w chmurze, faza 8). Faza 7 zacznie od checków na PR, przy okazji
  jeden obraz dla wszystkich serwisów Airflow zamiast osobnego builda per serwis.

**Git 23.10:** PR z 6.5 zmergowany (squash `15118df`), lokalna gałąź usunięta;
`git pull` sam usunął `origin/feat/retries-alerting` (gałąź na GitHubie
skasowana przy merge'u, `fetch.prune` już włączone).

**Zrobione (6.6, 23.10): backfill 11–13.09.**
- Przygotowanie: usunięty run testowy `manual__2026-09-13` (taski `extract_*`
  pod starymi ID sprzed TaskGroup; backfill wszedłby w ten istniejący run).
  `manual__2026-09-14` zostaje (patrz pytania niżej).
- Komenda: `docker compose exec airflow-scheduler airflow dags backfill
  ecommerce_pipeline -s 2026-09-11 -e 2026-09-13 2>&1 | tail -n 25` (`-s/-e` =
  które runy, obie granice włącznie; okna i zakładkę liczy Airflow). Wynik: 3/3
  runy, 24/24 taski sukces, ok. 4,5 min na dzień, dni po kolei
  (`depends_on_past` + `max_active_runs=1`).

| Dzień | Okno (z zakładką) | Wiersze z Postgresa | Walidacja | Iceberg po MERGE | dbt build |
|---|---|---|---|---|---|
| 11.09 | 10.09 23:50 → 12.09 00:00 | 18 | 0 w kwarantannie | 51 / 30 / 200 / 502 / 160 / 493 | `PASS=69 ERROR=0` |
| 12.09 | 11.09 23:50 → 13.09 00:00 | 21 | 0 | bez zmian | `PASS=69 ERROR=0` |
| 13.09 | 12.09 23:50 → 14.09 00:00 | 26 (jak test z 21.10) | 0 | bez zmian | `PASS=69 ERROR=0` |

- Freshness `WARN` każdego dnia (dane kończą się 15.09, oczekiwane). Żadnego
  nowego zadania w Asanie. Run 13.09 przetworzył to samo okno co
  `manual__2026-09-14` (run ręczny z datą D = okno D-1): ten sam dzień drugi raz,
  bez duplikatów.
- **Co backfill udowadnia, a czego nie.** Udowadnia: okna per dzień, zakładkę,
  bramkę jakości, kolejność dni oraz brak duplikatów i fałszywych wersji SCD2
  przy ponownym przetwarzaniu (`no changed rows -- nothing to insert`). Nie
  udowadnia przetwarzania „swojego dnia”: `merge_*` czytają CAŁY raw i
  deduplikują (`orders: incoming batch (deduped) = 200 rows` przy 18 wierszach z
  extractu), słabość znana z 6.4. Koszt przy backfillu 3 miesięcy: każdy dzień
  przetwarza całą dotychczasową historię (czas rośnie kwadratowo), a raw puchnie,
  bo każdy run dopisuje swoje pliki (append-only z `run_id`). Poprawka po 27.10:
  `merge_*` czytają tylko pliki bieżącego runu (ścieżki z extractu przez XCom
  albo filtr po `run_id`). To też odpowiedź na pytanie kontrolne z planu „jak
  zrobić backfill 3 miesięcy?”.
- Dowody zrobione 23.10: zrzut Grida (3 zielone runy `backfill__`) i zamkniętego
  zadania w Asanie.

**Pytania z tego kroku i czego uczą:**
- **Dlaczego nie usuwamy też runu 14.09?** (przed backfillem, po decyzji o
  usunięciu 13.09) Ma już nowe ID tasków (powtórzony 22.10 z TaskGroupami; brak
  mu tylko `dbt_source_freshness`, dodanego później), leży poza zakresem
  backfillu (dla `depends_on_past` 11.09 nie ma nic wcześniej, 12.09 patrzy na
  11.09, 13.09 na 12.09, nikt nie patrzy „w przód”) i jest dowodem pierwszego
  pełnego runu z dbt `PASS=51`. Usuwa się tylko to, co realnie przeszkadza.
- **Skąd „nowe” dane w Postgresie, skoro nikt do niego nie pisze?** (po wyniku
  backfillu) Nie ma nowych danych. `postgres-source` zawiera dane seed
  wygenerowane raz (`seed.py`) z datami w przeszłości do 15.09. Extract bierze
  wiersze, których znacznik czasu wpada w okno runu, więc backfill tylko kroi
  gotową historię na dni; 18/21/26 to liczba wierszy z danym dniem we wszystkich
  6 tabelach. W firmie źródło żyje (aplikacja ciągle pisze), a backfill
  odtwarza przeszłe dni z tych samych zasad okna.
- **Dlaczego liczba wierszy różni się między dniami, a Iceberg nie rośnie?** (jw.)
  Te wiersze są w Icebergu od fazy 3 (pierwsze pełne załadowanie). MERGE
  porównuje po kluczu: fakty z istniejącym kluczem nic nie dopisują, wymiary
  SCD2 tworzą nową wersję tylko przy zmianie atrybutu. Iceberg urośnie przy
  prawdziwej zmianie: nowe zamówienie (nowy klucz) albo klient zmieniający kraj
  (nowa wersja SCD2). Stąd pomysł na demonstrację w 6.7.

**Zrobione (6.7, część 1, 23.10): symulator ruchu w źródle.**
- Decyzja (Twoja): jeden plik `scripts/seed.py`, nie osobny skrypt; raz dziennie
  wystarczy (pipeline ma dzienne okno, dwie porcje i tak trafiłyby do jednego
  runu).
- `seed.py`: `--mode init` (dotychczasowy, logika bez zmian; historia, raz, na
  pustą bazę) i `--mode daily`: jeden dzień aktywności ze znacznikami między
  dzisiejszą 00:00 UTC a teraz — 1–3 nowych klientów (Faker), 1–2 zmiany kraju
  lub e-maila u starych klientów (nowe wersje SCD2), z szansą 50% zmiana ceny
  produktu o 5–20% (SCD2 + nowy snapshot ceny w dzisiejszych zamówieniach),
  5–15 nowych zamówień z pozycjami, historią statusów i w ok. 60% płatnością,
  3–8 starych otwartych zamówień o krok dalej (pending → paid z płatnością albo
  cancelled, paid → shipped, shipped → delivered). Historii nie zmienia, tylko
  dopisuje. Ziarno losowania: `init` 42, `daily` data (inne dane co dzień,
  powtarzalne w obrębie dnia). Zabezpieczenie dnia: zamówienie utworzone dziś =
  porcja zrobiona → nic nie robi (idempotencja; DAG widzą oba Airflowy). Jedna
  transakcja na całość. `DB_CONFIG` ze zmiennych środowiskowych jak w
  `extract.py` (było `localhost` na sztywno, decyzja 8).
- `dags/source_simulator.py`: jeden task `generate_daily_activity`
  (`BashOperator`), retry i alert do Asany jak w pipelinie, `catchup=False`
  (zdarzenia dostają znacznik „teraz”), harmonogram z configu: dev `30 23 * * *`
  (30 min przed runem dev o 00:00, którego okno [D, D+1) zawiera nowe wiersze),
  prod `null` (tylko ręcznie; oba środowiska czytają ten sam `postgres-source`).
  Nie jest taskiem `ecommerce_pipeline`: generowanie źródła to nie część
  pipeline'u (backfill przeszłości nie może dopisywać „dzisiejszych” danych).
- `airflow-docker/requirements.txt`: `faker` (wersja z constraints Airflow;
  lokalnie 40.39.0 z `uv.lock`).
- Test: z Maca `--mode daily` dwa razy → „1 new customers, 1 customer changes,
  1 price changes, 6 new orders, 8 order status transitions”, potem „already
  generated -- nothing to do”. Przebudowa obrazu (`--profile prod up -d
  --build`, 49 s, dev i prod odtworzone), `list-import-errors` pusto, `dags test
  source_simulator` na schedulerze dev → „already generated” + SUCCESS (`faker`
  w obrazie, połączenie przez zmienne środowiskowe, zabezpieczenie działa z
  kontenera). Oba DAG-i w dev `is_paused = False` (`unpause` wypisał `True`, czyli
  stan sprzed zmiany; sprawdzone przez `airflow dags list`).
- Pierwszy prawdziwy przyrost: run dev o 00:00 UTC 24.10 (okno 23.10) przetworzy
  wiersze wygenerowane 23.10 z Maca — wynik do zapisania 24.10.
- Ruff SIM117 (zagnieżdżone `with`) poprawione w `main()`: `with connection,
  connection.cursor() as cursor:`.
- Mój błąd: edycje `seed.py` zacząłem na `main` (w working tree, bez commita),
  gałąź `feat/source-simulator` powstała dopiero po Twojej uwadze. Zasada:
  gałąź przed pierwszą edycją.

**Pytania z tego kroku i czego uczą:**
- **Czy seed musi być historyczny i w osobnym pliku?** (pomysł generowania
  nowych rekordów z kombinacji imion, nazwisk i krajów) Nie: jeden plik z dwoma
  trybami. Historia już leży w wolumenie Postgresa, `init` jest tylko do
  zakładania środowiska od zera; tryb dzienny tylko dopisuje, więc backfill
  przeszłych dni dalej jest ważnym testem. W firmie danych z prod (PII) nie
  kopiuje się na dev: dane syntetyczne (Faker, SDV) albo zanonimizowana kopia;
  do systemu źródłowego się nie pisze (cudza aplikacja), u nas `postgres-source`
  ją udaje.
- **Raz czy dwa razy dziennie?** Raz: pipeline bierze okno całego dnia, więc dwie
  porcje trafiłyby do jednego runu. Dwa przebiegi mają sens dopiero przy
  pipelinie co kilka godzin (wtedy zabezpieczenie „ten dzień” → „ta połowa
  dnia”). Ograniczenie lokalne: Airflow działa, gdy Mac nie śpi; po wybudzeniu
  symulator wygeneruje dane z bieżącą datą.
- **Co dokładnie dodałeś w `seed.py`?** Dwa tryby (`argparse`), ziarno per tryb,
  `DB_CONFIG` z env, „pokrętła” `DAILY_*`, dotychczasowy `main()` jako
  `seed_initial()`, funkcje dnia: losowanie godzin zdarzeń bez przyszłości,
  wolny e-mail (`UNIQUE`), zabezpieczenie dnia, nowi klienci, zmiany klientów,
  zmiana ceny, nowe zamówienia (klient może zamówić dopiero po założeniu konta),
  postęp otwartych zamówień, `simulate_daily_activity()` składające dzień.
- **Dlaczego `--profile prod` tylko w pierwszej komendzie i czy dev też dostał
  `faker`?** Profil to flaga jednej komendy, nie stan. `up --build` tworzy i
  przebudowuje, więc musi objąć prod; `exec` wskazuje działający kontener po
  nazwie (`airflow-scheduler` = dev, `airflow-scheduler-prod` = prod). Dev też
  został odtworzony (`Started` przy serwisach bez `-prod`, `Running` = nietknięte),
  dowód: `dags test` na schedulerze dev zaimportował `faker`.
- **Dlaczego `--profile prod` uruchamia dev i prod i jak uruchomić sam prod?**
  Serwis bez `profiles:` jest aktywny zawsze, profil tylko DOKŁADA serwisy. Sam
  prod: nazwy serwisów w komendzie (`... up -d --build airflow-init-prod
  airflow-webserver-prod airflow-scheduler-prod`; `depends_on` dociąga bazę
  proda). Porządniej: profil `dev` dla serwisów dev (faza 7). W firmie dev i
  prod to osobne wdrożenia.
- **Skąd w VS Code „Env var required but not provided: DBT_SNOWFLAKE_ACCOUNT”?**
  Rozszerzenie dbt Power User parsuje projekt z profilem używającym `env_var()`,
  a od 22.10 zmienne są tylko w `.env`. dbt 1.12 sam wczytuje `.env`, ale tylko
  z bieżącego katalogu, a projekt dbt jest w `dbt_portfolio/`. Rozwiązanie bez
  kopiowania sekretów: `ln -s ../.env dbt_portfolio/.env` + przeładowanie okna
  (`.env` w `.gitignore` obejmuje też podkatalogi). W firmie: menedżer sekretów
  albo direnv. Pipeline tego nie dotyczy.
- **Co znaczy ostrzeżenie Ruff SIM117?** Dwa zagnieżdżone `with` można zapisać
  jako jedno z kilkoma kontekstami; działanie identyczne. W firmie takie uwagi
  zatrzymuje pre-commit hook i check w CI (faza 7).

**Git 23.10:** PR symulatora zmergowany (squash `84ece27`, #22). Weszła w nim też
zmiana `.gitignore`: wzorzec `DE_Interview_Cheatsheet*.docx` zamiast samego
`-2`, bo nowa kopia cheat sheetu `-3` nie była ignorowana.

**Zrobione (6.7, część 2, 23–24.10): pierwszy prawdziwy przyrost przez cały pipeline.**
- Ręczny run `airflow dags test ecommerce_pipeline 2026-10-24` (okno 23.10, 22:00
  UTC): 57 wierszy z Postgresa (2 klientów, 1 produkt, 14 zamówień = 6 nowych +
  8 zmienionych statusów, 15 pozycji, 7 płatności, 18 wpisów historii),
  walidacja bez kwarantanny, dbt `PASS=69 ERROR=0`.

| Tabela w Icebergu | Przed | Po | Skąd różnica |
|---|---|---|---|
| customer (wszystkie wersje) | 51 | 53 | +1 nowy klient, +1 wersja SCD2 (zmiana e-maila) |
| product (wszystkie wersje) | 30 | 31 | +1 wersja SCD2 (zmiana ceny) |
| orders | 200 | 206 | +6 nowych (8 przejść statusów aktualizuje istniejące) |
| order_items | 502 | 517 | +15 pozycji |
| payments | 160 | 167 | 4 opłacone tego samego dnia + 3 przejścia pending → paid |
| order_status_history | 493 | 511 | 6 × pending + 4 × paid + 8 przejść |

- Snowflake: `fct_orders` ma 6 nowych zamówień (201–206: 4 `paid` z
  `paid_amount` = wartość, 2 `pending` z 0). SCD2 w tabeli Iceberg: klient 39
  (zmiana e-maila) ma dwie wersje, `valid_to` starej = `valid_from` nowej, nowa
  `is_current = TRUE`; klient 51 (nowy) jedną. Analityk widzi 0 z 6 nowych
  zamówień — żadne nie jest z DE (WS, IT, CA, KG, LV, ID), czyli RLS działa na
  świeżych wierszach. Liczniki `COUNT(*)` per rola do powtórzenia pojedynczo
  (worksheet przy kilku zaznaczonych zapytaniach pokazuje tylko ostatni wynik).
- Nocny run z harmonogramu `scheduled__2026-10-23` (00:00–00:01:29 UTC): to samo
  okno drugi raz → znów 57 wierszy, tabele bez zmian (53 / 31 / 206 / 517 / 167 /
  511), SCD2 `no changed rows -- nothing to insert`, dbt `PASS=69`.
  Idempotentność potwierdzona na prawdziwym przyroście.

**Odkryte przy tym:**
- **`dags test` obok działającego schedulera.** Run ląduje we wspólnej bazie
  metadanych, a scheduler (DAG włączony) też zaczyna wykonywać jego taski:
  `extract_github` wykonał się 3 razy („Executor reports task instance …
  finished (success) although the task says it's queued”). Szkody brak dzięki
  idempotencji (raw z GitHuba dostał trzy kopie). Zasada: `dags test` tylko na
  wstrzymanym DAG-u albo w osobnym środowisku.
- **Kolizja dat.** Run ręczny z datą logiczną 24.10 zajmuje tę datę; scheduler nie
  utworzyłby swojego runu za 24.10 → cicha dziura w danych (żadnej porażki, więc
  żadnego alertu). `manual__2026-10-24` usunięty po zebraniu wyników (Browse →
  DAG Runs). Mój błąd: zaproponowałem tę komendę bez sprawdzenia skutków dla
  kolejnych runów.
- **Niepełne okno.** GitHub: 627 wierszy przy runie o 22:00 UTC (dzień 23.10 jeszcze
  trwał), 635 w nocy. Dlatego run za dzień D startuje dopiero D+1 o 00:00.
- **Trigger `set_updated_at` w Postgresie** (`ddl.sql`) przy każdym `UPDATE` ustawia
  `updated_at = now()`, więc godziny zmian podawane przez symulator są
  nadpisywane prawdziwym czasem zmiany: `valid_from` nowej wersji klienta 39 =
  20:59:56 UTC (moment uruchomienia). Realistyczne (tak działa prawdziwa
  aplikacja); do poprawy komentarz w `seed.py`, który sugeruje losowe godziny.
- **`Duration` ok. miesiąca przy `manual__2026-09-14`:** artefakt `dags test` (Start
  Date = data logiczna 14.09, End Date = faktyczny koniec 22.10); naprawdę ok.
  5 min. Run z harmonogramu ma normalne 1 min 29 s.
- **`-0700` w wynikach Snowflake'a:** `TIMESTAMP_LTZ` wyświetlany w strefie sesji
  (domyślnie Los Angeles); 13:59:56 -0700 = 20:59:56 UTC. Staging dbt
  konwertuje do UTC, więc modele od tego nie zależą.

**Pytania z tego kroku i czego uczą:**
- **Czy teraz mam dwa DAG-i?** (po dodaniu symulatora) Tak, każdy Airflow wczytuje
  oba pliki: `source_simulator` (dev 23:30 UTC, prod tylko ręcznie) i
  `ecommerce_pipeline` (dev 00:00, prod 03:00). Łączy je tylko czas, bez jawnej
  zależności — celowo, bo symulator gra cudzą aplikację. W firmie przy zależności
  między własnymi DAG-ami: Datasets (data-aware scheduling, Airflow 2.4+),
  `TriggerDagRunOperator`, `ExternalTaskSensor`; przy cudzym źródle: okno z
  zakładką + sprawdzanie świeżości.
- **Czy możemy sprawdzić przyrost od razu, bez czekania na noc?** Technicznie tak
  (`dags test` z datą 24.10), ale z dwoma skutkami ubocznymi opisanymi wyżej
  (podwójne wykonanie przez scheduler, zajęta data). Właściwy sposób: poczekać na
  run z harmonogramu albo przeliczyć zamknięty dzień backfillem/Clear.
- **Dlaczego to jest „test”?** Nazwa myli: `dags test` = uruchom cały DAG raz,
  teraz, w terminalu, bez schedulera; taski działają naprawdę (S3, Iceberg,
  Snowflake). Brak retry z przerwą, brak `max_active_runs`. W firmie: narzędzie
  developera lokalnie i w CI; na prod runy uruchamia tylko scheduler.
- **Dlaczego w `dags test` podaje się datę dnia następnego?** W Airflow 2 data runu z
  harmonogramu/backfillu = dzień danych (początek okna), run startuje po jego
  końcu. W runie ręcznym podana data = moment uruchomienia, a Airflow przypisuje
  ostatni zakończony dzień przed nią: 24.10 → okno 23.10. Trigger z datą z
  przyszłości jest domyślnie zablokowany (`allow_trigger_in_future = False`).
- **Dlaczego jeden run w DAG Runs trwa miesiąc?** Artefakt `dags test` (wyżej).

**Zrobione (6.7, część 3a, 24.10): testy `dbt-expectations` na `fct_orders`.**
- `packages.yml`: `metaplane/dbt_expectations` 0.10.10 (utrzymywana kontynuacja
  zarchiwizowanego `calogica/dbt_expectations`; dbt >=1.7 <3.0), wersja
  przypięta jak `dbt_utils`; `dbt deps` dociągnął zależność `dbt_date` 0.21.0,
  `package-lock.yml` zaktualizowany. `dbt_packages/` w `.gitignore`; Airflow
  widzi pakiet bez przebudowy obrazu, bo `dbt_portfolio/` jest zamontowany (w
  firmie `dbt deps` w CI/przy budowie obrazu — faza 7).
- Trzy testy w `models/marts/schema.yml`, każdy łapie coś, czego dotychczasowe
  nie widziały:

| Test | Kolumna | Co łapie | Poziom |
|---|---|---|---|
| `expect_row_values_to_have_recent_data` (≤ 2 dni) | `order_date` | cichy zator: mart przestał dostawać dane, choć żaden task nie padł (lekcja z 6.5) | `warn` |
| `expect_column_values_to_match_regex` (`^[A-Z]{2}$`) | `country_at_order` | złamany kontrakt formatu ze źródła; RLS filtruje po tej wartości, więc zły format = błędne uprawnienia | `error` |
| `expect_column_values_to_be_between` (0–10 000) | `gross_order_value` | anomalia wartości (zdublowane pozycje, zła cena); pułap z kształtu danych: ceny 5–500, max 4 pozycje × 3 szt. ≈ 6000 | `warn` + `store_failures` |

- Pierwsze uruchomienie: test świeżości `ERROR` (`Required var
  'dbt_date:time_zone' not found`). Naprawa: `vars: 'dbt_date:time_zone': 'UTC'`
  w `dbt_project.yml` (UTC, bo staging konwertuje wszystkie znaczniki do UTC).
  Drugie uruchomienie w kontenerze dev (`dbt test --select fct_orders --target
  dev`): `PASS=20 WARN=0 ERROR=0`. Ostatnia praca wymagająca Snowflake'a przed
  końcem triala zrobiona.
- `seed.py`: komentarz o triggerze `set_updated_at` (wszystkie tabele poza
  `order_status_history`): godziny podawane w UPDATE są nadpisywane przez
  `now()`, losowe czasy działają tylko przy INSERT i w `changed_at` historii.

**Pytania z tego kroku i czego uczą:**
- **Co znaczy „długość komendy to cena uruchamiania dbt w kontenerze”?** (przy
  komendzie testów) Sam test to `dbt test --select fct_orders --target dev`;
  reszta to dojście do dbt w kontenerze: `docker compose exec airflow-scheduler`
  (tam profil i zmienne są gotowe: `DBT_PROFILES_DIR` na zamontowany profil,
  zmienne z `.env`, klucz tylko do odczytu), `bash -c` (kilka poleceń naraz),
  `cd` do zamontowanego projektu, pełna ścieżka do dbt (osobny venv poza
  `PATH`). Na Macu krótka wersja nie działa, bo dbt czyta `.env` tylko z
  bieżącego katalogu (`dbt_portfolio/`), a `.env` leży w korzeniu repo; pomaga
  dowiązanie `ln -s ../.env dbt_portfolio/.env`. W firmie developer ma profil i
  dostęp skonfigurowane lokalnie (albo dbt Cloud IDE / devcontainer) i pisze
  `dbt test -s fct_orders`; długa ścieżka to sposób schedulera, nie człowieka.
- **Skąd wiadomo, że test padł na kompilacji, zanim cokolwiek sprawdził?** (po
  pierwszym `ERROR`) Trzy sygnały: treść `Compilation Error` (dbt najpierw
  kompiluje Jinja/YAML do SQL, potem wykonuje SQL w hurtowni — tu SQL nie
  powstał); czas `0.02s` wobec 0.7–1.1 s pozostałych (za mało na zapytanie do
  Snowflake'a); wynik `ERROR`, nie `FAIL`. `FAIL n` = test się wykonał i znalazł
  n złych wierszy (problem w danych); `WARN` = to samo przy `severity: warn`;
  `ERROR` = testu nie dało się wykonać (kompilacja, brak tabeli, błąd SQL —
  problem w kodzie/konfiguracji). W firmie od tego zależy, kto reaguje: `FAIL` —
  zespół danych/właściciel źródła, `ERROR` — autor ostatniej zmiany w kodzie.
  Lekcja: filtr `grep` zjadł linię z powodem błędu; pełny powód jest w
  `dbt_portfolio/logs/dbt.log`.
- **Dlaczego `dbt_date:time_zone` = UTC?** Test świeżości porównuje `order_date` z
  „teraz”; oba muszą być w tej samej strefie, a staging trzyma wszystko w UTC.
  Strefę biznesu (np. `Europe/Warsaw`) ustawia się, gdy „dzień” w raportach ma
  się liczyć według czasu klienta.
- **Czy testy dbt są w ogóle uruchamiane w Snowflake'u?** (po zielonym `dbt
  test`) Tak, w całości. dbt jest kompilatorem i dyrygentem SQL-a: zamienia
  test z YAML-a na `SELECT` zwracający ZŁE wiersze, Snowflake wykonuje go na
  warehouse'ie (kredyty — dlatego po 27.10 testy dbt staną), dbt liczy tylko
  zwrócone wiersze (0 = `PASS`, n = `FAIL n`/`WARN n`). Skompilowany SQL każdego
  testu leży w `dbt_portfolio/target/compiled/`. Testy z `store_failures` najpierw
  zapisują złe wiersze w schemacie `dbt_dev_dbt_test__audit` (`create or replace
  transient table …`), potem liczą `count(*)` z tej tabeli; unit test tworzy z
  fixture'ów pustą tabelę tymczasową (`where false limit 0`), żeby poznać typy
  kolumn. W firmie identycznie — testy danych działają tam, gdzie dane leżą; w
  odróżnieniu od `validate.py`, który czyta surowe pliki z S3 w Pythonie (bramka
  przed hurtownią).
- **Gdzie w Snowflake'u widać, że zapytanie wysłał dbt?** (przy przeglądaniu
  Query History) Trzy ślady: (1) komentarz doklejany przez dbt na końcu każdego
  SQL-a: `/* {"app": "dbt", "dbt_version": …, "target_name": "dev", "node_id":
  "test.dbt_portfolio.…"} */` — `node_id` wskazuje konkretny test/model; (2)
  CLIENT DRIVER `PythonConnector` (dbt-snowflake) vs `Go` (Snowsight, czyli
  ręczne zapytania z worksheetu); (3) wspólne SESSION ID zapytań jednego
  uruchomienia. U nas USER to Twój użytkownik (dbt loguje się Twoim kluczem); w
  firmie dbt ma użytkownika serwisowego (np. `SVC_DBT`) i `query_tag` w profilu
  (np. `dbt_prod`), dzięki czemu w Query History i w rozliczeniu kredytów od razu
  widać, co zużył dbt, a co ludzie (`query_tag` — faza 7). Wyniki zapytań
  `COUNT(*)` per rola nie są widoczne w historii — do dowodów uruchomić je
  pojedynczo w worksheecie.

**Następny krok:** commit z testami wypchnięty (24.10); drugi commit w tym samym
PR: te pytania, `CLAUDE.md` (symulator, `dbt_expectations`, pułapki `dags
test`, nazwa cheat sheetu) i nowe sekcje cheat sheetu 31.26–31.29 (lokalnie,
poza repo). Po merge'u, do 26.10: zrzuty dowodów (liczniki `COUNT(*)` per rola
pojedynczo), README na pytaniach kontrolnych z planu. Po 27.10: `merge_*` tylko
na plikach bieżącego runu, auto-zamykanie i deduplikacja zadań w Asanie,
`dagrun_timeout` z alertem DAG-a; faza 7 (CI, Ruff w pre-commit, `dbt deps` w
CI, `query_tag` i użytkownik serwisowy dla dbt, jeden obraz Airflow, profil
`dev` w compose).

---

*Ten plik aktualizuj po każdym większym kroku — kolejność chronologiczna; pytania z rozmowy zapisane z kontekstem (skąd się wzięły) i z wiedzą, której uczą, tak żeby były zrozumiałe po miesiącu; ogólna teoria w cheat sheecie.*
