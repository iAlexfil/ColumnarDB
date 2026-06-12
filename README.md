# ColumnarDB

Колоночная аналитическая СУБД, выполняющая все 43 запроса бенчмарка [ClickBench](https://github.com/ClickHouse/ClickBench) на полном датасете `hits` (≈100 млн строк, 105 колонок).

Написана на C++23. Сборка через CMake, тесты на GoogleTest. Из внешних зависимостей — только [RE2](https://github.com/google/re2) (для одного запроса с регулярными выражениями).

Проект не реализует SQL-парсер: каждый из 43 запросов ClickBench собран вручную из операторов исполнительного движка (как если бы планировщик уже отработал). Это позволяет сфокусироваться на самом интересном — на хранении колонок и физическом исполнении.

---

## Оглавление

1. [Что вообще делает программа](#что-вообще-делает-программа)
2. [Полный жизненный цикл данных](#полный-жизненный-цикл-данных)
3. [Как запустить](#как-запустить)
4. [Структура проекта](#структура-проекта)
5. [Система типов](#система-типов)
6. [Хранение в памяти: Batch и DictColumn](#хранение-в-памяти-batch-и-dictcolumn)
7. [Дисковый формат `.columnar`](#дисковый-формат-columnar)
8. [Чтение: mmap и readahead](#чтение-mmap-и-readahead)
9. [Исполнительный движок: Volcano-модель](#исполнительный-движок-volcano-модель)
10. [Операторы](#операторы)
11. [Выражения](#выражения)
12. [Скалярные функции](#скалярные-функции)
13. [Агрегация (HashAggregate подробно)](#агрегация-hashaggregate-подробно)
14. [HashMap и SplitMix64](#hashmap-и-splitmix64)
15. [SIMD](#simd)
16. [RE2 для регулярных выражений](#re2-для-регулярных-выражений)
17. [Как собираются 43 запроса](#как-собираются-43-запроса)
18. [Запись результата](#запись-результата)
19. [Сборка (CMake)](#сборка-cmake)
20. [Тесты](#тесты)
21. [Результаты на ClickBench](#результаты-на-clickbench)

---

## Что вообще делает программа

Есть два исполняемых файла:

- **`csv_to_columnar`** — берёт CSV-файл с данными (`hits.csv`) + файл со схемой (`hits.schema`), и записывает их в свой бинарный колоночный формат `.columnar`. Это делается один раз.

- **`clickbench_run`** — читает `.columnar` файл и выполняет на нём 43 запроса ClickBench, записывая результат каждого запроса в отдельный CSV (`q00.csv` ... `q42.csv`).

Идея колоночного хранения: в аналитических запросах обычно нужны не все 105 колонок, а 2-3. Если хранить данные построчно (как в CSV), для подсчёта `COUNT(*) WHERE AdvEngineID <> 0` пришлось бы прочитать весь файл. В колоночном формате каждая колонка лежит отдельным непрерывным блоком, поэтому читается только нужная колонка `AdvEngineID` — остальные 104 не трогаются.

---

## Полный жизненный цикл данных

Проследим путь одной строки `hits` от CSV до результата запроса.

### Шаг 1. CSV → columnar (конвертация)

`hits.csv` — это 100 млн строк вида:
```
7091626910923651030,1,"Заголовок страницы",1,"2013-07-02 00:16:46","2013-07-02",40367,...
```

`csv_to_columnar`:
1. Читает `hits.schema` — список из 105 пар `(имя_колонки, тип)`.
2. Читает CSV построчно (`getline` + 4 MB буфер).
3. Накапливает строки в `Batch` — 65536 строк за раз.
4. Когда батч заполнен, `ColumnarWriter` записывает его на диск: каждая колонка — отдельным блоком, с выбором кодировки (Plain / Dict / RLE).
5. В конце пишет footer: схему + позиции (offset, size, encoding) всех блоков.

### Шаг 2. Исполнение запроса

Возьмём Q1: `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0`.

В коде это собирается как дерево операторов:
```
Scan(["AdvEngineID"])  →  Filter(AdvEngineID != 0)  →  HashAggregate(COUNT)  →  Project(count)
```

Исполнение идёт «снизу вверх», но управляется «сверху вниз» — верхний оператор тянет данные у нижнего методом `Next()`:

1. **Project.Next()** просит данные у **HashAggregate**.
2. **HashAggregate.Next()** в цикле тянет все батчи у **Filter**, считая строки.
3. **Filter.Next()** тянет батч у **Scan**, вычисляет предикат `AdvEngineID != 0`, оставляет только подходящие строки (через selection vector — без копирования).
4. **Scan.Next()** читает с диска один батч колонки `AdvEngineID` (только её, остальные 104 колонки не читаются).

### Шаг 3. Результат

`HashAggregate` собрал одну группу с `count = 14174`. `Project` оставил колонку `count`. Результат записывается в `q01.csv`:
```
count
14174
```

---

## Как запустить

### Зависимости

```bash
bash script/setup.sh
# ставит: git build-essential cmake g++ libre2-dev
```

### Сборка

```bash
bash script/build.sh
```

Бинари: `build/build/Release/{csv_to_columnar, clickbench_run, ColumnarDB}`.

### Конвертация

```bash
./build/build/Release/csv_to_columnar \
  --input hits.csv --schema hits.schema --output hits.columnar
```

### Все 43 запроса

```bash
./build/build/Release/clickbench_run \
  --input hits.columnar --output_dir results/
```

Результаты — `results/q00.csv` ... `results/q42.csv`.

### Один запрос

```bash
./build/build/Release/clickbench_run \
  --input hits.columnar --output_dir results/ --queries=7
```

### Через скрипты (для CI / Docker)

```bash
bash script/setup.sh                                # зависимости
bash script/build.sh                                # сборка
bash script/convert.sh <csv> <columnar>             # конвертация
bash script/run_query.sh <N> <columnar> <out> <log> # один запрос N
```

### Тесты

```bash
cd build/build/Release && ctest
```

---

## Структура проекта

```
src/
  utils/
    utils.h          — DataType, DataObject, DataVector, Trim, Seek
    parse.h          — парсинг int/float/Date/DateTime и их форматирование
    hash_map.h       — кастомная open-addressing hash map
    simd.h           — AVX2-обёртки (сравнение и арифметика int64/double)
  csv/
    csvreader.{h,cpp} — чтение CSV (RFC 4180: кавычки, экранирование)
    csvwriter.{h,cpp} — запись CSV
  schema/
    schema.h         — ColumnSchema, Schema, загрузка/сохранение схемы
  engine/
    batch/
      batch.{h,cpp}      — Batch (набор колонок в памяти) + CsvBatchReader
      dict_column.h      — DictColumn (dictionary-encoded строки)
    columnar/
      columnar_format.h    — константы формата, BatchMeta, Encoding
      columnar_writer.{h,cpp} — запись .columnar
      columnar_reader.{h,cpp} — чтение через mmap
    exec/
      operator.h     — базовый класс Operator
      exec_batch.h   — ExecBatch (Batch* + selection vector)
      expr.{h,cpp}   — базовый класс Expr + фабрики Make*
      exprs/         — конкретные выражения (column, compare, arith, ...)
      scan.h         — оператор Scan
      filter.h       — оператор Filter
      project.h      — оператор Project
      hash_aggregate.{h,cpp} — оператор HashAggregate
      sort.h         — оператор Sort
      topk.h         — оператор TopK
      limit.h        — оператор Limit
      func.{h,cpp}   — реестр скалярных функций (length, like, extract, ...)
bench/
  clickbench_run.cpp — 43 захардкоженных плана + main
exe/
  csv_to_columnar.cpp — конвертер CSV → columnar
main.cpp             — вспомогательный CLI (to-columnar / to-csv)
tests/
  test_types.cpp     — типы и парсинг
  test_roundtrip.cpp — columnar write → read даёт исходные данные
  test_exec.cpp      — операторы и выражения
hits.schema          — схема таблицы hits (105 колонок)
```

---

## Система типов

### DataType — типы колонок (13 штук)

```cpp
enum class DataType : uint8_t {
    Int64, String, Int8, Int16, Int32,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64, Date, DateTime
};
```

- **Date** хранится как `int32` — число дней с 1970-01-01.
- **DateTime** хранится как `int64` — число секунд с epoch.

Типы читаются из `hits.schema` (CSV `имя,тип`). Парсинг типа — `ParseColumnType` в `schema.h` (`"int64"` → `DataType::Int64`, плюс алиасы `timestamp`→DateTime, `char`→String).

### DataVector — одна колонка в памяти

```cpp
using DataVector = std::variant<
    std::vector<int8_t>, std::vector<int16_t>, ...,
    std::vector<double>,
    DictColumn        // для строк
>;
```

Числовые колонки — обычный `std::vector<T>`. Строковые — `DictColumn` (dictionary encoding, см. ниже).

### EvalType — типы на этапе исполнения (7 штук)

При вычислении выражений узкие типы расширяются до широких, чтобы не плодить кодовые пути:

```cpp
enum class EvalType { I64, U64, F64, Str, Bool, Date, DateTime };
```

- `Int8/16/32/64` → `I64` (всё считается в `int64`)
- `UInt8/16/32/64` → `U64`
- `Float32/64` → `F64`
- `Date`, `DateTime` хранятся как `int64`, но помечены своим EvalType (чтобы при выводе отформатировать как дату)
- `Bool` — результат сравнений и логики, хранится как `vector<uint8_t>`

### EvalCol — результат вычисления выражения

```cpp
using EvalCol = std::variant<
    std::vector<int64_t>, std::vector<uint64_t>,
    std::vector<double>, std::vector<std::string>,
    std::vector<uint8_t>   // Bool
>;
```

Любое выражение `eval()` возвращает один столбец значений в одном из этих пяти представлений.

---

## Хранение в памяти: Batch и DictColumn

### Batch

`Batch` — это набор колонок одинаковой длины + их схема:

```cpp
class Batch {
    Schema schema_;
    std::vector<DataVector> columns_;
    std::size_t row_count_;
};
```

Размер батча — 65536 строк. Весь движок работает батчами: Scan читает батч, Filter фильтрует батч, и т.д. Это компромисс между построчной обработкой (много overhead на вызовы) и обработкой всей таблицы сразу (не влезет в память / cache).

### DictColumn — dictionary encoding строк

Прямолинейное хранение строк — `vector<string>` — расточительно: в колонке `URL` строка `http://example.com` может повторяться миллионы раз, и каждая копия — отдельная аллокация.

`DictColumn` хранит каждую уникальную строку **один раз**:

```cpp
class DictColumn {
    std::deque<std::string> dict_;          // уникальные строки (словарь)
    std::vector<uint32_t> codes_;           // для каждой строки — индекс в словаре
    HashMap<string_view, uint32_t> index_;  // обратный индекс: строка → код
};
```

- `operator[](i)` → `dict_[codes_[i]]` — O(1)
- `push_back(s)` — ищет `s` в `index_`; если есть, добавляет код, иначе создаёт новую запись в словаре. O(1) в среднем.

Почему `std::deque`, а не `vector` для словаря: `index_` хранит `string_view`, указывающие на строки в `dict_`. У `vector` при росте происходит реаллокация и все `string_view` становятся висячими. У `deque` элементы не двигаются при добавлении — указатели остаются валидными.

**Эффект:** на колонках с повторами (URL, Title, SearchPhrase) — экономия памяти в 5-10 раз. Дополнительно: dictionary encoding бесплатно ускоряет `GROUP BY` по строкам — группировка идёт по `uint32` кодам, а не по самим строкам.

---

## Дисковый формат `.columnar`

Версия 3. Magic-байты `"CDB1"`.

### Общая структура файла

```
┌──────────────────────────┐
│ Header (16 байт)          │  "CDB1" + version(uint32) + footer_offset(uint64)
├──────────────────────────┤
│ Batch 0                   │
│   Column 0 (encoded)      │  кодировка выбирается per-column, per-batch
│   Column 1 (encoded)      │
│   ...                     │
├──────────────────────────┤
│ Batch 1                   │
│   ...                     │
├──────────────────────────┤
│ ...                       │
├──────────────────────────┤
│ Footer                    │  схема + метаданные всех батчей
└──────────────────────────┘
```

`footer_offset` в заголовке указывает где начинается footer. Сначала пишется 0 (плейсхолдер), а в конце — патчится реальное значение через seek.

### Footer

```
ncols (uint32)
для каждой колонки:
    name (uint32 длина + байты)
    type (uint8)
nbatches (uint32)
для каждого батча:
    row_count (uint32)
    для каждой колонки:
        offset (uint64)    — где блок начинается в файле
        size (uint64)      — длина блока в байтах
        encoding (uint8)   — 0=Plain, 1=Dict, 2=RLE
```

Footer читается первым (через `footer_offset`), это даёт reader'у карту всех блоков. После этого любой блок читается одним seek'ом.

### Кодировка Plain (числовые колонки без повторов)

```
[raw bytes: sizeof(T) * row_count]
```

Просто массив значений как есть. Используется когда RLE не даёт выигрыша.

### Кодировка Dict (все строковые колонки)

```
dict_size (uint32)              — число уникальных строк в батче
для каждой строки словаря:
    len (uint32) + байты
codes (uint32 * row_count)      — код каждой строки
```

Это тот же dictionary encoding что и в памяти, сериализованный на диск.

### Кодировка RLE (числовые колонки с повторами)

```
num_runs (uint32)
для каждого run'а:
    value (T)
    count (uint32)              — сколько раз value повторяется подряд
```

Writer считает число run'ов и сравнивает:
```
rle_size   = 4 + num_runs * (sizeof(T) + 4)
plain_size = row_count * sizeof(T)
```
Если RLE компактнее — пишет RLE, иначе Plain. Решение принимается отдельно для каждой колонки каждого батча.

RLE отлично работает на колонках, отсортированных или с низкой кардинальностью: `EventDate` (события одного дня идут подряд), `Sex`, `IsRefresh`, `AdvEngineID` (часто 0).

### Экономия места на полном hits (75 GB исходного CSV)

| Конфигурация | Размер |
|---|---|
| Plain (всё как есть) | ≈45 GB |
| + Dict для строк | ≈30 GB |
| + RLE для чисел | **≈23 GB** |

---

## Чтение: mmap и readahead

`ColumnarReader` не использует `fread`/`ifstream` для данных. Вместо этого он маппит весь файл в адресное пространство:

```cpp
fd_ = open(path, O_RDONLY);
mapped_ = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd_, 0);
```

Теперь файл доступен как обычный массив байт `mapped_[offset]`. Операционная система сама подгружает страницы с диска по мере обращения (page fault) и кэширует их.

**Чтение колонки** = `memcpy` из mmap-региона в `DataVector` (для Plain), либо разворачивание Dict/RLE. Это одна копия (kernel page cache → наш vector), без промежуточного буфера ifstream.

**Проекция:** `Scan` читает только нужные колонки. Если запрос использует 2 из 105 колонок, остальные 103 блока в файле просто не трогаются — соответствующие страницы даже не загружаются с диска.

**Readahead (упреждающее чтение):** перед чтением батча N оператор Scan просит ядро асинхронно подгрузить батч N+1:

```cpp
readahead(fd_, chunk.offset, chunk.size);
```

Пока операторы обрабатывают батч N (CPU-работа), диск параллельно читает батч N+1. Когда дойдём до N+1 — данные уже в page cache, обращение мгновенное. Получается перекрытие I/O и вычислений (software pipelining).

---

## Исполнительный движок: Volcano-модель

Классическая pull-based (тянущая) модель. Каждый оператор реализует интерфейс:

```cpp
class Operator {
    virtual std::optional<ExecBatch> Next() = 0;     // выдать следующий батч (или nullopt)
    virtual const Schema& OutputSchema() const = 0;  // схема того, что выдаёт
};
```

Запрос — дерево операторов. Чтобы получить результат, вызывают `Next()` у корня. Корень вызывает `Next()` у своего ребёнка, тот у своего, и так до листа (Scan). Данные «текут» снизу вверх по одному батчу.

### ExecBatch и selection vector

Оператор выдаёт не сам Batch, а лёгкую обёртку:

```cpp
struct ExecBatch {
    const Batch* batch;            // указатель на данные (не владеет)
    std::vector<uint32_t> sel;     // индексы активных строк; пустой = все строки
};
```

**Selection vector** — ключевая оптимизация. Когда Filter отсеивает строки, он **не копирует** прошедшие строки в новый Batch. Вместо этого он формирует `sel` — список индексов строк, которые прошли фильтр:

```
batch:  [10, 0, 25, 0, 7, 0, 13]   ← колонка AdvEngineID
sel:    [0, 2, 4, 6]                ← индексы где значение != 0
```

Операторы выше работают только по этим индексам. Данные остаются на месте — zero-copy фильтрация. Если `sel` пустой, значит активны все строки батча (быстрый путь без индирекции).

### Почему батчи, а не строки

Если бы `Next()` выдавал по одной строке, на 100 млн строк было бы 100 млн виртуальных вызовов через всю цепочку операторов — это сотни миллионов indirect jumps. Батч в 65536 строк амортизирует этот overhead в 65 тысяч раз: один `Next()` = обработка 65536 строк в тесном цикле (который ещё и SIMD-векторизуется).

---

## Операторы

Все наследуют `Operator`. Их семь:

```
Operator (абстрактный)
├── Scan          — источник: читает батчи из ColumnarReader
├── Filter        — отсеивает строки по предикату (Bool-выражению)
├── Project       — вычисляет новые колонки из выражений
├── HashAggregate — GROUP BY + агрегатные функции
├── Sort          — полная сортировка (+ LIMIT/OFFSET)
├── TopK          — top-K строк через min-heap (для ORDER BY + LIMIT)
└── Limit         — первые N строк
```

### Scan

Лист дерева, источник данных. Три способа указать какие колонки читать:

```cpp
Scan(reader)                           // все колонки
Scan(reader, {0, 3, 7})                // по индексам
Scan(reader, {"URL", "UserID"})        // по именам
```

Каждый `Next()` читает один следующий батч (через `ColumnarReader::ReadBatchColumns`), предварительно запустив `readahead` для следующего. Когда батчи кончились — возвращает `nullopt`.

### Filter

Принимает дочерний оператор и предикат (выражение с `result_type() == Bool`). На каждый батч:
1. Вычисляет предикат → маска `vector<uint8_t>`.
2. Собирает `sel` из индексов где маска = 1.
3. Возвращает ExecBatch с этим `sel` (данные не копируются).

Если после фильтра не осталось строк — берёт следующий батч (не выдаёт пустой).

### Project

Принимает список пар `{имя, выражение}`. На каждый батч вычисляет каждое выражение и формирует **новый** Batch с новой схемой. Используется для `SELECT a, b+1, length(c)` — то есть когда колонки на выходе отличаются от входных.

### Sort

Блокирующий оператор: при первом `Next()` вытягивает **все** батчи ребёнка в один большой Batch, сортирует, применяет LIMIT/OFFSET. Сортировка — `std::stable_sort` по вектору индексов (не двигая сами данные), потом материализация результата по отсортированным индексам.

Поддерживает несколько ключей сортировки с направлением (ASC/DESC) для каждого.

### TopK

Для `ORDER BY ... LIMIT k` без OFFSET. Эффективнее полного Sort когда `k` мало: вместо сортировки всех 100M строк держит min-heap размера `k`. Для каждой строки: если она «лучше» худшей в heap — заменяет худшую. В конце heap сортируется для финального порядка. Сложность O(n log k) вместо O(n log n).

### Limit

Простой: выдаёт первые N строк и дальше возвращает `nullopt`.

### Tiebreakers (детерминизм результата)

При `ORDER BY count LIMIT 10`, если у многих групп одинаковый `count`, какие именно 10 попадут в результат — формально не определено. Чтобы результат был воспроизводимым, `AddSort`/`AddTopK` автоматически дописывают **все** колонки выходной схемы как вторичные ключи сортировки (ASC). Это делает выбор top-N каноническим.

---

## Выражения

Выражение вычисляет одну колонку значений из батча. Базовый класс:

```cpp
class Expr {
    virtual EvalType result_type() const = 0;
    virtual EvalCol eval(const EvalContext&) const = 0;
};
```

`EvalContext` = указатель на Batch + опциональный selection vector. Метод `rows()` возвращает число активных строк.

### Восемь видов выражений

```
Expr (абстрактный)
├── ColumnExpr    — читает колонку из батча по индексу
├── ConstExpr<T>  — константа (вектор из N одинаковых значений)
├── CompareExpr   — сравнение: Eq, Ne, Lt, Le, Gt, Ge
├── ArithExpr     — арифметика: Add, Sub, Mul, Div, Mod
├── LogicalExpr   — логика: And, Or, Not
├── InListExpr    — IN (список констант)
├── IfExpr        — CASE WHEN cond THEN x ELSE y
└── FuncCallExpr  — вызов скалярной функции (length, like, ...)
```

### Фабрики (factory functions)

Конкретные классы выражений объявлены в `exprs/*.h` и не экспортируются. Создаются через фабрики из `expr.h`:

```cpp
ExprPtr MakeColumnByName(const Schema& s, string_view name);
ExprPtr MakeConstI64(int64_t v);
ExprPtr MakeCompare(ExprPtr l, CmpOp op, ExprPtr r);
ExprPtr MakeArith(ExprPtr l, ArithOp op, ExprPtr r);
ExprPtr MakeLogical(LogOp op, vector<ExprPtr> args);
ExprPtr MakeInList(ExprPtr lhs, vector<ExprPtr> consts);
ExprPtr MakeIf(ExprPtr cond, ExprPtr t, ExprPtr f);
ExprPtr MakeFuncCall(const string& name, vector<ExprPtr> args);
```

`ExprPtr = unique_ptr<Expr>`. Дерево выражений владеет своими поддеревьями через `unique_ptr`.

Зачем фабрики: пользователю (коду в clickbench_run.cpp) не нужно знать о классах `CompareExpr` и т.п. — он работает с абстрактным `Expr`. Конкретные реализации спрятаны в `.h` файлах папки `exprs/`, подключаемых только в `expr.cpp`.

### Промотирование типов

Когда сравниваются/складываются два разных числовых типа, `CommonNumericType` определяет общий:
- если хотя бы один `F64` → результат `F64`
- `I64` и `U64` → `F64` (безопасно, без переполнения знака)
- одинаковые → тот же тип
- Date/DateTime трактуются как `I64`

`CastEval<T>(col)` приводит EvalCol к `vector<T>` через `static_cast`.

### Пример: как вычисляется `AdvEngineID <> 0`

```
CompareExpr(Ne)
├── ColumnExpr("AdvEngineID")   → читает колонку → vector<int64>
└── ConstExpr(0)                → vector<int64>{0, 0, ...}
```
`CompareExpr::eval` вычисляет оба ребёнка, приводит к общему типу (I64) и делает поэлементное `!=` (через SIMD) → `vector<uint8_t>` маска.

---

## Скалярные функции

Функции вроде `length`, `like`, `extract` зарегистрированы в синглтоне `FuncRegistry` с перегрузками по имени и типам аргументов:

```cpp
struct ScalarFn {
    std::string name;
    std::vector<EvalType> arg_types;
    EvalType result_type;
    std::function<EvalCol(const std::vector<EvalCol>&, size_t rows)> impl;
};
```

`MakeFuncCall("like", args)` ищет подходящую перегрузку по типам аргументов и оборачивает в `FuncCallExpr`.

### Реализованные функции

| Функция | Сигнатура | Описание |
|---|---|---|
| `length` | (Str) → I64 | длина строки в байтах |
| `like` | (Str, Str) → Bool | SQL LIKE с `%` (любая подстрока) и `_` (один символ) |
| `extract` | (Str, DateTime) → I64 | извлечь year/month/day/hour/minute/second |
| `date_trunc` | (Str, DateTime) → DateTime | округлить вниз до minute/hour/day |
| `regexp_replace` | (Str, Str, Str) → Str | замена по регулярке (через RE2) |

### LIKE-паттерн

Реализован итеративным алгоритмом с backtracking (greedy match `%` с откатом). Оптимизация: если паттерн одинаковый для всех строк (а в ClickBench всегда — `LIKE '%google%'`), он анализируется один раз на батч, а не на каждую строку.

---

## Агрегация (HashAggregate подробно)

Самый сложный оператор. Реализует `GROUP BY` + агрегатные функции. Это **блокирующий** оператор: при первом `Next()` он прочитывает все данные ребёнка, и только потом отдаёт результат.

### Агрегатные функции

```
GroupAgg (абстрактный)
├── CountStarAgg        — COUNT(*)
├── SumAgg<T>           — SUM(expr)
├── MinMaxAgg<T>        — MIN/MAX(expr)
├── AvgAgg<T>           — AVG = SUM/COUNT
└── CountDistinctAgg<T> — COUNT(DISTINCT expr) через unordered_set
```

Каждый агрегат хранит состояние **по группам**. Например `CountStarAgg` — `vector<uint64_t> counts_`, где `counts_[g]` = число строк в группе `g`. Метод `UpdateBatch(gids, ctx)` получает для каждой строки батча номер её группы (`gids`) и обновляет состояние.

### Как строится группировка

```cpp
HashMap<Key, uint32_t> map;   // ключ группы → номер группы
uint32_t num_groups = 0;

для каждого батча:
    вычислить ключевые колонки
    для каждой строки r:
        собрать ключ k из значений ключевых колонок
        если k нет в map:
            gid = num_groups++       // новая группа
            map[k] = gid
            расширить состояние всех агрегатов
        gids[r] = map[k]
    обновить агрегаты по gids
```

Для скалярных агрегатов без `GROUP BY` (например `SELECT COUNT(*)`) используется синтетический ключ `__zero = 0` — все строки попадают в одну группу.

### Два пути: быстрый и общий

**Быстрый путь** (`ConsumeFastImpl<N>`) — когда ключей ≤ 4 и среди них нет float. Ключ упаковывается в `FastKeyN<N>` — это `std::array<int64_t, N>`, лежащий на стеке без аллокаций:

```cpp
template<std::size_t N>
struct FastKeyN { std::array<int64_t, N> data; };
```

`N` выбирается по числу ключей (1, 2, 3 или 4), так что для `GROUP BY x` ключ занимает 8 байт, а не 32. Это критично для Q32 (`GROUP BY WatchID, ClientIP` — ~100M уникальных пар): меньше ключ → меньше памяти → запрос влезает в RAM.

**Строки в быстром пути** обрабатываются через runtime dictionary encoding: каждая уникальная строка ключа интернируется в общий пул (`InternString`), и в `FastKeyN` кладётся её `uint32` код. Группировка идёт по числам.

**Общий путь** (`Consume`) — когда ключей > 4 или есть float. Ключ — `vector<variant<...>>`, hash-таблица — наш `HashMap<GroupKey, uint32_t>`.

### Сборка результата

После обработки всех батчей формируется выходной Batch: для каждой группы пишутся значения ключей + результаты агрегатов (`agg->EmitInto(column)`).

---

## HashMap и SplitMix64

`src/utils/hash_map.h`. Кастомная open-addressing хеш-таблица с квадратичным пробированием. Заточена под наш единственный use case: ключ → `uint32_t` (номер группы / код строки).

```cpp
template<class Key, class Hash>
class HashMap {
    uint32_t* find(const Key&);          // nullptr если нет
    void insert(const Key&, uint32_t);
    void for_each(Fn) const;
    void clear();
};
```

Особенности:
- **Open-addressing** — все записи в одном плоском массиве (cache-friendly), без отдельных нод как в `std::unordered_map` (там каждая запись — отдельный `malloc` и прыжок по указателю).
- **Quadratic probing**: при коллизии пробуем слоты `(h + 1), (h + 3), (h + 6), ...` — формула `(h + step*(step+1)/2) & mask`. Избегает первичной кластеризации линейного пробирования.
- **Power-of-2 размер** → индекс через `& mask` вместо деления.
- **Load factor ≤ 50%** — растём при заполнении на половину.
- **Sentinel**: пустой слот помечен значением `UINT32_MAX`. Нет `erase` — он не нужен для агрегации.

### SplitMix64 — почему не std::hash

`std::hash<int64_t>` обычно тождественная — возвращает само число. Для последовательных ключей (а коды из dictionary encoding — это 0, 1, 2, 3, ...) при power-of-2 размере таблицы это даёт катастрофическую кластеризацию: все ключи ложатся в соседние слоты, пробирование вырождается в O(n).

Хеш-функция перемешивает биты алгоритмом SplitMix64 (три раунда xor-shift + умножение):

```cpp
v ^= v >> 33;
v *= 0xff51afd7ed558ccd;
v ^= v >> 33;
v *= 0xc4ceb9fe1a85ec53;
v ^= v >> 33;
```

Даёт идеальный avalanche: смена 1 бита на входе меняет ~50% битов на выходе. Соседние числа дают абсолютно разные хеши. Константы — те же, что в финализаторе MurmurHash3.

Без этого один из запросов (`GROUP BY SearchPhrase` после интернирования) уходил в практически бесконечный probe loop.

---

## SIMD

`src/utils/simd.h`. AVX2-обёртки для горячих циклов в выражениях. Обрабатывают 4 значения `int64`/`double` за одну инструкцию.

### Сравнение int64

```cpp
__m256i a = _mm256_loadu_si256(...);   // загрузить 4 int64
__m256i b = _mm256_loadu_si256(...);
__m256i eq = _mm256_cmpeq_epi64(a, b); // поэлементное ==
```

Реализованы `CmpEqI64` и `CmpGtI64`. Все 6 операторов сравнения выражаются через них: Lt = Gt с обменом аргументов, Le/Ge/Ne = отрицание (xor с 1).

### Арифметика

`AddI64`, `SubI64` для целых; `AddF64`, `SubF64`, `MulF64` для double.

Подключены в `compare.h::DispatchCmp` (для int64) и `arith.h::ArithLoop` (int64 и double). Каждая функция имеет scalar tail для остатка `n % 4`.

### Где даёт эффект

На полном hits (батчи по 65536):
- фильтры по int-колонкам (Q1, Q19, Q36-Q42) — ускорение ~1.5-2×
- арифметика (Q29 — 90 раз `SUM(ResolutionWidth + N)`, Q35 — `ClientIP - 1/2/3`) — ~2-3×

**Сравнение double через SIMD не сделано осознанно:** все фильтры в 43 запросах идут по целочисленным колонкам, ни один не сравнивает float. Поэтому SIMD-путь для double был бы кодом, который никогда не выполняется. Float участвует только в агрегатах (AVG), где сравнения нет.

Требуется CPU с AVX2 (Intel с 2013, AMD с 2015); включается флагом `-march=native` в Release.

---

## RE2 для регулярных выражений

Q28 использует `REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')` — вытаскивает домен из URL.

Стандартный `std::regex` (ECMAScript, NFA с backtracking) на 100M строк работает ~280 секунд — он печально известен медлительностью. Подключена Google [RE2](https://github.com/google/re2): Thompson NFA без backtracking, гарантированно O(n) на матч. Тот же запрос — **~190 секунд** (−32%).

```cpp
RE2 re(pattern);            // компилируется один раз на батч
RE2::Replace(&str, re, repl);
```

Capture groups в формате RE2 (`\1`) используются напрямую, без конвертации. Линкуется как `libre2` (пакет `libre2-dev`).

---

## Как собираются 43 запроса

В `bench/clickbench_run.cpp` каждый запрос — функция `Plan QN(ColumnarReader&)`, собирающая дерево операторов через мини-DSL.

### Структура Plan

```cpp
struct Plan {
    std::vector<std::unique_ptr<Operator>> nodes;          // владеет всеми операторами
    Operator* root = nullptr;                              // текущая вершина дерева
    std::unordered_map<std::string, DataType> format_hints; // подсказки форматирования вывода
};
```

`nodes` хранит владение операторами. `AddXxx(p, ...)` создаёт оператор, делает его ребёнком текущего `root` и обновляет `root`. Так дерево строится снизу вверх.

### Пример: Q7

`SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC`

```cpp
Plan Q7(ColumnarReader& rdr) {
    Plan p;
    AddScan(p, rdr, {"AdvEngineID"});
    AddFilter(p, Ne(C(p, "AdvEngineID"), MakeConstI64(0)));
    AddHashAgg(p,
        Cols(KV{"AdvEngineID", C(p, "AdvEngineID")}),  // ключ группировки
        Aggs(CountStar("count")));                      // агрегат
    AddSort(p, SortKeys(SK{C(p, "count"), false}));     // DESC по count
    AddProject(p, Cols(
        KV{"AdvEngineID", C(p, "AdvEngineID")},
        KV{"count", C(p, "count")}));
    return p;
}
```

Хелперы DSL:
- `C(p, "name")` — `ColumnExpr` по имени из текущего `root`
- `Ne/Eq/Ge/Le`, `Add/Sub/Mul`, `AndN`, `InList`, `Like`, `Length`, ... — конструируют выражения
- `Cols(...)`, `Aggs(...)`, `SortKeys(...)` — variadic-сборка списков
- `CountStar/Sum/Min/Max/Avg/Distinct` — спецификации агрегатов

### Запуск

`main` парсит аргументы (`--input`, `--output_dir`, опционально `--queries=N,M`), открывает `ColumnarReader`, и для каждого выбранного запроса строит план, гоняет `root->Next()` до конца, пишет результат в `q{NN}.csv`, замеряет время.

---

## Запись результата

`WritePlanToCsv` вытягивает все батчи из корня плана и пишет CSV:

- **числа** — `std::to_string` для целых, `%.15g` для double
- **строки** — всегда в кавычках `"..."`
- **Date** — через `format_hints` форматируется как `YYYY-MM-DD`
- **DateTime** — как `YYYY-MM-DD HH:MM:SS`

`format_hints` нужен потому что Date/DateTime в памяти — это `int64`, и без подсказки они вывелись бы числом. План явно помечает такие колонки (например Q6 помечает `min`/`max` как Date).

---

## Сборка (CMake)

### Библиотеки

| Цель | Тип | Содержимое |
|---|---|---|
| `utils` | INTERFACE | Trim, Seek, parse, hash_map, simd |
| `csv` | STATIC | CSV reader/writer |
| `schema` | INTERFACE | DataType, схема |
| `batch` | STATIC | Batch, DictColumn, CsvBatchReader |
| `columnar` | STATIC | ColumnarWriter, ColumnarReader (mmap) |
| `exec` | STATIC | операторы, выражения, функции (линкует `re2`) |

### Исполняемые файлы

| Цель | Назначение |
|---|---|
| `csv_to_columnar` | конвертер CSV → columnar |
| `clickbench_run` | прогон 43 запросов |
| `ColumnarDB` | вспомогательный CLI (to-columnar / to-csv) |

### Флаги

- **Release**: `-O3`, LTO (`CMAKE_INTERPROCEDURAL_OPTIMIZATION`), `-march=native` (включает AVX2)
- **Debug**: `-fsanitize=address,undefined`, `-fno-omit-frame-pointer`

---

## Тесты

80 unit-тестов (GoogleTest), три файла:

- **test_types.cpp** — DataType, парсинг int/float/Date/DateTime, Trim, схема
- **test_roundtrip.cpp** — запись Batch в columnar и чтение обратно даёт исходные данные (включая edge cases: кавычки в CSV, пустые строки, CRLF, большие строки, битые файлы)
- **test_exec.cpp** — все операторы (Scan/Filter/Project/Sort/TopK/HashAggregate) и выражения (compare, arith, logical, in_list, if, функции), а также комбинации (Filter→Aggregate, GROUP BY, HAVING)

```bash
cd build/build/Release && ctest
```

---

## Результаты на ClickBench

Полный датасет: 99 997 497 строк, 105 колонок. Платформа: 19 GiB RAM, 8 vCPU, NVMe SSD.

**43/43 запросов выполняются, 0 падений.**

### Типичные времена (cold run)

| Запрос | Время | Что делает |
|---|---|---|
| Q0 | 0.4s | COUNT(*) |
| Q1 | 1.5s | COUNT WHERE AdvEngineID<>0 |
| Q7 | 1.5s | GROUP BY + ORDER BY |
| Q15 | 5.8s | GROUP BY UserID (17M групп) |
| Q18 | 55s | GROUP BY UserID, minute, SearchPhrase |
| Q28 | 189s | REGEXP_REPLACE + HAVING (RE2) |
| Q32 | 48s | GROUP BY WatchID, ClientIP (~100M групп) |
| Q33 | 77s | GROUP BY URL, топ-10 |

Geomean по 43 запросам: **≈9.5 секунды**.

### Вклад оптимизаций (geomean)

| Шаг | Geomean | Δ |
|---|---|---|
| База (Plain encoding, std::unordered_map) | ≈25s | — |
| + Dictionary encoding | ≈16s | −36% |
| + FastKeyN<N> (типизированные ключи групп) | ≈13s | −19% |
| + mmap + readahead | ≈12s | −8% |
| + кастомный HashMap + SplitMix64 | ≈11s | −8% |
| + RLE encoding | ≈10s | −9% |
| + SIMD (AVX2) | ≈9.7s | −3% |
| + RE2 | **≈9.5s** | −2% |

### Сравнение с DuckDB

Наш однопоточный движок в среднем **~7-10× медленнее** DuckDB (многопоточный, с SIMD и проприетарным сжатым форматом). Для учебного проекта без многопоточности это сильный результат — top-3 среди реализаций на курсе.

Основной нереализованный резерв ускорения — **многопоточность** (параллельный scan + локальные хеш-таблицы по потокам с финальным merge), которая дала бы ещё ~4-8× на многоядерной машине.
