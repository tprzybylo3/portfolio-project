# ADR-003: AI-Assisted Engineering Workflow — Claude Code + Codex + Hooks + Failure Log

## Status
Accepted

## Kontekst

Osobny wątek od głównej architektury data platformy (patrz `project-summary-for-consultation.md`,
ADR-002). Celem jest wykorzystanie nowoczesnych AI coding agentów (Claude Code, Codex) jako
realnego elementu profesjonalnego workflow inżynierskiego przy budowie tego projektu — nie jako
osobny "AI demo project", tylko jako udokumentowany sposób pracy nad już zaplanowaną architekturą.

Pierwotna propozycja (research własny + konsultacja z ChatGPT) obejmowała szeroki zestaw
elementów: Claude Code jako główny implementer, Codex jako niezależny reviewer, wielorundowy
adversarial debate między nimi z formalnym dokumentem `architecture_review.md` przy każdej
zmianie, custom MCP server do inspekcji Airflow/Snowflake/Iceberg, Claude Code hooks jako
guardraile, Skills per-domena (dbt, PySpark, Iceberg), subagenci (SQL reviewer, security
reviewer, docs agent), Agent Teams, i AI-driven architecture regression testing.

Po konsultacji i researchu (obecne możliwości Claude Code 2026, wzorce cross-provider review
Claude↔Codex, dostępność oficjalnych MCP serverów dla Snowflake/Airflow, sygnały cenione przez
recruiterów/senior DE w portfolio projektach) zakres został świadomie ograniczony.

Kluczowe ustalenia z researchu:

- **Redakcja przy handoff do review jest krytyczna dla realnej niezależności.** Jeśli reviewer
  (Codex) widzi reasoning implementera (Claude), ocena przestaje być niezależna — reviewer
  "freeloaduje" na cudzej ramie myślowej i częściej się zgadza. Prawdziwie niezależny review
  wymaga przekazania tylko kodu/diffu + wymagań, bez uzasadnienia.
- **Oficjalne MCP servery dla Snowflake i Airflow już istnieją** (Snowflake-managed MCP server,
  GA; `apache-airflow-providers-common-ai` z natywną obsługą MCP) — budowa własnego serwera od
  zera dla tych systemów byłaby reinventing the wheel bez uzasadnienia.
- **Sygnał ceniony przez senior/staff DE i recruiterów w 2026 to udokumentowany osąd inżynierski**
  (real decisions, tested, documented failures, explainable pod presją rozmowy) — nie szerokość
  orkiestracji agentów. Rozbudowana architektura multi-agent jako *core* część portfolio DE
  ryzykuje odwrotny efekt: sygnalizuje entuzjazm dla AI kosztem umiejętności DE.

## Decyzja

**Główny workflow (część głównego repo):**

```
Ty → zadanie
Claude Code (Plan Mode: explore → plan → implement) → implementacja
Hooks (deterministyczne guardraile: blokada destrukcyjnego SQL, sekretów) → allow/block
Testy deterministyczne (pytest / dbt test / Great Expectations) → walidacja
Codex (redagowany handoff — tylko kod/diff, BEZ reasoningu Claude'a) → niezależny review
Claude Code → poprawki
Failure log (jeśli wystąpił realny incydent) → ai/failure-log/NNN-opis.md
Ty → decyzja → ADR (jeśli decyzja architektoniczna)
```

1. **Claude Code jako główny implementer**, sterowany przez `CLAUDE.md` + Plan Mode
   (explore → plan → implement → test → verify), zgodnie z oficjalnie zalecanym wzorcem.
2. **Hooks jako deterministyczne guardraile od startu** — minimalny, konkretny zestaw:
   blokada destrukcyjnych komend SQL/bash (`DROP TABLE`, `rm -rf`, force-push), blokada
   zapisu sekretów do repo, uruchamianie testów po zmianie kodu.
3. **Codex jako niezależny reviewer, z wymuszoną redakcją handoffu** — Codex dostaje tylko
   kod/diff i wymagania, nigdy uzasadnienia/reasoningu Claude'a. Jedna runda review na
   istotny PR, nie wielorundowa debata.
4. **Failure log** (`ai/failure-log/NNN-opis.md`) — dokumentuje *realne* incydenty (błąd AI →
   jak wykryty → root cause → poprawka → prewencja), z polem `## Detection layer`
   (hook / test / Codex / ręcznie), żeby z czasem pokazać mierzalną ewolucję: coraz więcej
   błędów łapanych deterministycznie, coraz mniej ręcznie. Tylko realne przypadki, nie na
   siłę do jakiejś liczby wpisów (4–6 dobrze opisanych > 15 naciąganych).
5. **Regresja architektoniczna PASS/FAIL z dowodem**, powiązana z ADR — w praktyce dobrze
   zorganizowany zestaw testów mapowany na wymagania z ADR (np. "idempotent ingestion" →
   `test_incremental_rerun.py`, `test_duplicate_batch.py`), nie osobna "warstwa AI".
6. **AI diagnozuje, deterministyka wykrywa** — przy incydentach data quality, LLM dostaje
   gotowe sygnały (deviation %, freshness, duplicates) i formułuje hipotezę + evidence,
   nigdy sam nie decyduje, czy dane są poprawne.

**Osobny folder eksperymentalny (`experiments/`), poza głównym workflow:**

```
experiments/
├── mcp/              (custom MCP server — dopiero gdy pojawi się konkretna luka,
│                       czyli moment "Claude nie ma dostępu do informacji X i przez
│                       to nie może dobrze zdiagnozować problemu"; nie budowany na start)
├── subagents/
├── agent-teams/
└── adversarial-review/   (wielorundowy Claude↔Codex debate protocol)
```

## Rozważane i odrzucone

- **Wielorundowy adversarial debate z formalnym `architecture_review.md` przy każdej
  zmianie jako core workflow** — odrzucone dla core repo. Dla solo-projektu to teatr
  procesu; jedna dobrze zrobiona runda review > udokumentowana "debata" w stylu
  korporacyjnym dla jednoosobowego repo. Zostaje jako eksperyment.
- **Custom MCP server budowany na starcie** — odrzucone. Oficjalne, w pełni wspierane
  MCP servery dla Snowflake i Airflow już istnieją. Własny server ma sens dopiero
  z konkretnym, nieobsłużonym use case'em (np. cross-referencing Airflow + Iceberg +
  dbt w jednym query).
- **Subagent army / Agent Teams jako fundament** — odrzucone dla core repo. Wartość
  rośnie przy pracy zespołowej lub bardzo dużym repo; koszt to zduplikowany kontekst
  i koordynacja bez proporcjonalnej korzyści dla solo-projektu.
- **Osobne Skills per-domena (dbt/PySpark/Iceberg) jako duże dokumentacyjne dumpy** —
  odrzucone w tej formie. Krótki `CLAUDE.md` + hooki tam, gdzie reguła musi być
  wymuszona deterministycznie, pokrywa większość potrzeb na tym etapie.

## Konsekwencje

**Pozytywne:**
- Workflow uczy się na realnym, działającym przykładzie zamiast budować scaffolding
  wokół pustego projektu
- Failure log daje narrację "AI popełnia błąd → wykrywamy → rozumiemy dlaczego →
  dodajemy test/guardrail → ten sam błąd staje się niemożliwy" — mocniejszy sygnał
  inżynierski niż lista narzędzi AI
- Redakcja handoffu do Codex daje realną, nie pozorowaną niezależność review
- Jasne rozgraniczenie core vs experiments/ chroni główny projekt przed przerostem
  formy nad treścią

**Negatywne / trade-offy:**
- Rezygnacja z pełnej wizji multi-agent orchestration z pierwotnej propozycji — część
  ambitniejszych elementów (Agent Teams, MCP) odłożona do momentu, gdy pojawi się
  konkretne uzasadnienie
- Wymaga dyscypliny przy pisaniu failure logu — łatwo pominąć go "bo szkoda czasu",
  a wtedy traci się najmocniejszy element tej warstwy

## Powiązane

- `project-summary-for-consultation.md` — architektura główna data platformy
- ADR-002 — strategia Bronze/ingestion, na której będzie testowany pierwszy przepływ
  Claude Code + hooks + Codex review (skrypt seedujący + incremental extract do S3)

## Następny krok

Pierwszy realny test workflow: skrypt seedujący Faker dla 7 tabel `postgres-source`,
pisany przez Claude Code z hookiem blokującym destrukcyjny SQL od startu, następnie
jedna runda redagowanego review przez Codex.

## Retrospekcja (dodane w trakcie Fazy 4)

**Obserwacja:** "Następny krok" powyżej zakładał start workflow już od `seed.py`
(Faza 1). W praktyce Fazy 1-4 (ingestion, walidacja, PySpark/Iceberg, Snowflake)
zostały zaimplementowane bezpośrednio w rozmowie z Claude (chat), bez formalnego
Claude Code/Codex workflow — bez Plan Mode, bez hooków, bez redagowanego review,
bez wpisów do `ai/failure-log/`. To realna luka między deklaracją ADR-003 a tym,
co faktycznie się wydarzyło, nazwana wprost, nie ukryta.

**Decyzja:** nie nadrabiać retroaktywnie — kod z Faz 1-4 jest już napisany,
przetestowany i zweryfikowany na żywych danych; przepisywanie go wyłącznie po to,
żeby zgadzało się z procesem, nie daje proporcjonalnej wartości względem czasu,
który lepiej wydać na Fazy 5-8. Workflow z ADR-003 zaczyna faktycznie obowiązywać
**od Fazy 5 (dbt)** — pierwszy moment przejścia na Claude Code (implementacja) +
Codex (review) zostanie jawnie zasygnalizowany w rozmowie projektowej, z gotowym
`CLAUDE.md` przygotowanym zanim nastąpi przełączenie narzędzi.

**Uzasadnienie, czemu to nie jest tylko wymówka:** ta rozmowa (chat) pełni funkcję
warstwy projektowej/decyzyjnej (planowanie, ADR-y, debugowanie na żywo w konsoli
AWS/Snowflake) — nie jest, i nigdy nie miała być, substytutem Claude Code. Fazy
1-4 były w praktyce pracą eksploracyjną (pierwszy kontakt z Iceberg/Snowflake,
dużo pytań "dlaczego"), gdzie wartość brała się z konwersacyjnego uczenia się,
nie z automatyzacji. Fazy 5-8 (dbt/Airflow/CI/CD/Terraform) to bardziej
skonsolidowana implementacja znanych już wzorców — naturalnie lepiej pasujący
moment na formalny workflow implementacyjny z guardrailami.
