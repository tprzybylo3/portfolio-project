# ADR-006: Okno ekstrakcji — data interval z zakładką zamiast watermarku w stanie

## Status
Accepted (decyzja). Wdrożenie w Fazie 6, krok 6.3 (refaktor `extract.py` i
`github_extract.py`), weryfikacja w kroku 6.6 (test backfillu).

## Kontekst

Do Fazy 5 oba skrypty ekstrakcji trzymały watermark w stanie: lokalne pliki
`scripts/state/watermark_state.json` i `github_watermark_state.json`. Każdy run
czytał ostatnią wartość, pobierał nowsze wiersze i nadpisywał stan
`MAX(updated_at)` z pobranego batcha. Docstring `extract.py` zapowiadał, że w
Fazie 6 stan przejdzie do Airflow Variables (PROGRESS_LOG §5).

Airflow wprowadza dwa wymagania, których watermark w stanie nie spełnia dobrze:

1. **Backfill i ponowne uruchomienie.** Ruchomy watermark jest już przesunięty
   do przodu, więc przeliczenie 3 października wymaga ręcznego cofnięcia stanu i
   przywrócenia go po wszystkim. Pomyłka w tej sekwencji gubi albo dubluje dane.
2. **Audytowalność.** Watermark to jedna nadpisywana wartość. Nie ma śladu, który
   run przetworzył jaki zakres.

Dodatkowo istniejący watermark ma lukę niezależną od orkiestracji: w Postgresie
`now()` (i `DEFAULT now()` na `updated_at`) zwraca czas **startu** transakcji, nie
commitu. Transakcja zaczęta przed odczytem, a zatwierdzona po nim, dostaje
`updated_at` mniejsze od nowego watermarku i wiersz nigdy nie zostaje pobrany.

## Decyzja

1. **Okno ekstrakcji = data interval runu Airflow.** Skrypt dostaje
   `--start`/`--end` z template'ów (`data_interval_start`/`data_interval_end`) i
   filtruje `updated_at >= start AND updated_at < end` (dla
   `order_status_history`: `changed_at`). Skrypt nie zna Airflow i da się go
   odpalić ręcznie z dowolnym zakresem.
2. **Zakładka (lookback) na początku okna**, doliczana w tasku:
   `data_interval_start - 10 min`. Długość zakładki musi przekraczać najdłuższą
   transakcję zapisującą w źródle. Koniec okna zostaje ostry.
3. **Żadnego stanu watermarku.** `scripts/state/*.json` i Airflow Variables nie
   są używane do ekstrakcji. Historię runów i ich okien trzyma baza metadanych
   Airflow (`dag_run`).
4. **Duplikaty z zakładki i ponownych runów zjada warstwa niżej**, bez zmian w
   kodzie: `merge_facts.py`/`merge_dimensions.py` deduplikują batch po PK
   (najnowszy `updated_at` wygrywa) przed `MERGE`. Raw w S3 zostaje append-only
   z `run_id` (ADR-002).
5. **GitHub API, per encja** (API nie ma jednolitego filtra „od-do”):
   - `issues`: `since = start` po stronie serwera, `updated_at < end` po stronie
     klienta.
   - `pull_requests`: sortowanie `updated desc`, stop na pierwszym elemencie
     `< start`, filtr `< end` po stronie klienta.
   - `repositories`, `contributors`: bez okna, pełny snapshot w każdym runie
     (brak `updated_at` albo pojedynczy obiekt).

## Rozważane alternatywy

**A: Watermark w Airflow Variables** (plan z docstringu `extract.py`).
Odrzucone: przenosi problem z pliku do bazy Airflow, ale nie rozwiązuje go.
Backfill nadal wymaga ręcznego cofania stanu, a równoległe runy ścigałyby się o
jedną zmienną.

**B: Watermark jako `MAX(updated_at)` z tabeli docelowej** (Iceberg/Snowflake,
wariant opisany w PROGRESS_LOG §5). Samonaprawiający się (brak osobnego stanu do
zgubienia), ale backfill konkretnego dnia nadal niemożliwy bez nadpisania logiki,
a ekstrakcja zależy od dostępności warstwy docelowej.

**C: Log-based CDC (Debezium czytający WAL).** Jedyne rozwiązanie bez okien i
zakładek, łapiące też pośrednie wartości i DELETE. Odrzucone dla tego projektu:
osobna infrastruktura (Kafka/Debezium), nieproporcjonalna do celu portfolio.
Opisane w cheat sheecie (6, 24.2, 31.2) jako docelowa odpowiedź na pytanie „jak to
naprawić całkowicie”.

## Konsekwencje

**Pozytywne:**
- Każdy run jest idempotentny: to samo okno daje te same wiersze ze źródła, a
  `MERGE` z deduplikacją daje ten sam stan Silver.
- Backfill zakresu to `airflow dags backfill -s ... -e ...` albo Clear w UI, bez
  ręcznej edycji stanu.
- Luka „późnego commitu” jest zamknięta przez zakładkę.
- Mniej ruchomych części: znika plik stanu do zgubienia albo uszkodzenia.

**Negatywne / do pamiętania:**
- **Pierwszy załadunek** wymaga świadomej decyzji: `start_date` DAG-a
  obejmujący najstarsze dane (`catchup=True`, wiele runów) albo jednorazowy run z
  `--start` w odległej przeszłości.
- **Pośrednie wartości nadal giną** (OLTP nadpisuje w miejscu, cheat sheet 24.2).
  Okna tego nie naprawiają, to ograniczenie CDC opartego na stanie tabeli.
- **Backfill źródła API nie odtwarza przeszłości.** GitHub zwraca bieżący stan
  obiektów, więc stary run pobiera tylko te, których *obecne* `updated_at` mieści
  się w jego oknie. Idempotentny (bez duplikatów), ale nie reprodukowalny
  historycznie. `contributors`/`repositories` w backfillu dają stan z dnia
  uruchomienia.
- Zakładka zwiększa wolumen raw o duplikaty z ~10 minut na run. Przy tej skali
  pomijalne.
- Kolejność runów: `max_active_runs=1`, żeby backfill szedł chronologicznie.

## Powiązane

- ADR-002 (append-only raw + idempotentny `MERGE`, na którym ta decyzja polega)
- PROGRESS_LOG §5 (pierwotny watermark), §10 (ograniczenia watermarku), §26
- Cheat sheet: 6 (CDC), 19.6 (MVCC), 24.2, 31 (data interval, zakładka, backfill)
