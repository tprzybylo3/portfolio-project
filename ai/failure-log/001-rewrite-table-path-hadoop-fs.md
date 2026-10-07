# 001: `rewrite_table_path` nie działa w kontenerze Sparka bez Hadoop S3 FileSystem

**Data:** 2026-10-07
**Zadanie:** przeniesienie 6 tabel Iceberg do ścieżek S3 zgodnych z nazwami (ADR-005)
**Agent:** Claude Code
**Wpływ:** brak na dane. Pilotaż na `product` zatrzymał się przed jakąkolwiek zmianą w
Glue i w katalogu docelowym. Zostały 4 przepisane pliki metadanych w
`s3://tp-portfolio-ecommerce-raw/migration-staging/2026-10-07/product/`. Nic ich nie
referencjonuje i leżą poza `warehouse/`.

## Błąd AI

W planie zarekomendowałem opcję (b): `rewrite_table_path` + `register_table`. Uznałem
ją za wykonalną, bo zweryfikowałem tylko, że procedura jest w jarze
(`RewriteTablePathProcedure` w `iceberg-spark-runtime-3.5_2.12-1.9.1`, parametry z
`javap`). Nie sprawdziłem, czy da się ją uruchomić w tej konfiguracji kontenera, a
plan obiecywał "weryfikuj, nie zakładaj".

## Jak wykryto

Pierwsze uruchomienie `relocate.py prepare product`, czyli zaplanowany pilotaż na
tabeli najniższego ryzyka. Błąd:
`org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "s3"`
w `RewriteTablePathSparkAction.saveFileList`.

## Root cause

Akcja Sparka zapisuje przepisane metadane przez `FileIO` tabeli (`S3FileIO`, działa),
ale listę plików do skopiowania zapisuje jako Spark Dataset przez **Hadoop
FileSystem**. Cały pipeline (Glue + `S3FileIO` z `iceberg-aws-bundle`) nigdy nie
potrzebował Hadoopowego dostępu do S3. Dlatego obraz ma tylko `hadoop-client` 3.3.4
z PySparka, bez `hadoop-aws`, i schemat `s3://` nie ma implementacji FileSystem.
Obecność procedury w jarze nie mówi nic o jej zależnościach w czasie wykonania.

## Poprawka

W sesji Sparka w `relocate.py` (bez zmiany obrazu):
`spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4` (wersja zgodna z Hadoopem
z PySparka 3.5.5), `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
i endpoint regionu. Do tego unikalny katalog stagingowy per uruchomienie
(`.../<tabela>/<run id>/`), żeby resztki nieudanego przebiegu nie blokowały
kolejnego i nie trzeba było niczego kasować w trakcie.

## Prewencja

- Przy weryfikacji narzędzia w planie rozróżniać "istnieje" od "działa tutaj".
  Najtańszy dowód to uruchomienie na obiekcie bez ryzyka przed zarekomendowaniem
  ścieżki. Tutaj tę rolę spełnił pilotaż, ale rekomendacja padła wcześniej.
- Przy akcjach Sparka z Icebergiem sprawdzać, czy zapisują wyniki przez `FileIO`
  tabeli, czy przez Spark/Hadoop. To dwie różne ścieżki dostępu do S3 w tym samym
  kontenerze.
- Wzorzec, który zadziałał: snapshot przed, pilotaż na tabeli bez polityk, kroki
  zapisujące tylko poza obiektami produkcyjnymi do momentu weryfikacji. Dzięki temu
  błąd środowiska nie dotknął danych.

## Druga odsłona tego samego błędu (ten sam pilotaż)

Po dodaniu `hadoop-aws` procedura przeszła, ale skrypt padł na `NoSuchKey`.
`file_list_location` to nie pojedynczy plik, tylko katalog wyjściowy Spark CSV
(`part-*.csv` + `_SUCCESS`). Root cause ten sam: założyłem sposób, w jaki akcja
zapisuje wyniki, zamiast to sprawdzić. Poprawka: `read_file_list` czyta wszystkie
`part-*.csv` z tego katalogu. Skrypt zatrzymał się przed kopiowaniem, więc katalog
docelowy pozostał pusty, a w Glue nic nie powstało.

## Detection layer

Ręcznie, przez zaplanowany pilotaż na żywym środowisku. Nie złapał go żaden hook,
test ani review. Pilotaż jest elementem procedury (CLAUDE.md, guardrails), a nie
deterministycznym testem.
