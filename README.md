# ColumnarDB

Колоночная СУБД для аналитических запросов. Выполняет все 43 запроса ClickBench на полном датасете hits (99M строк, 105 колонок).

C++23, CMake, GoogleTest. Без внешних зависимостей.

## Быстрый старт

### Требования

- Linux (Ubuntu 22.04+)
- CMake 3.25+
- g++ с поддержкой C++23

### Сборка

```bash
bash script/build.sh
```

Бинари появятся в `build/build/Release/`.

### Конвертация CSV в columnar-формат

```bash
bash script/convert.sh <input.csv> <output.columnar>
```

Или напрямую:

```bash
./build/build/Release/csv_to_columnar \
  --input hits.csv \
  --schema hits.schema \
  --output hits.columnar
```

### Запуск всех запросов

```bash
./build/build/Release/clickbench_run \
  --input hits.columnar \
  --output_dir results/
```

Результаты — CSV-файлы `q00.csv` ... `q42.csv`.

### Запуск одного запроса

```bash
./build/build/Release/clickbench_run \
  --input hits.columnar \
  --output_dir results/ \
  --queries=7
```

### Запуск через скрипты (для Docker/CI)

```bash
bash script/setup.sh              # зависимости
bash script/build.sh              # сборка
bash script/convert.sh <csv> <columnar>  # конвертация
bash script/run_query.sh <N> <columnar> <output.csv> <log>  # один запрос
```

### Тесты

```bash
cd build/build/Release
ctest
```

80 unit-тестов: типы, парсинг, roundtrip columnar, все операторы и выражения.

---

## Архитектура

### Обзор

```
hits.csv
   |
   v
csv_to_columnar  ──>  hits.columnar  (колоночный бинарный формат)
                            |
                            v
                     clickbench_run   (исполнение 43 запросов)
                            |
                            v
                      q00.csv ... q42.csv
```

Система состоит из четырёх уровней:

1. **Storage** — колоночный бинарный формат (чтение/запись)
2. **Batch** — единица данных в памяти (набор колонок фиксированного размера)
3. **Execution engine** — pull-based volcano model (операторы + выражения)
4. **Query runner** — захардкоженные планы 43 запросов ClickBench

### Структура директорий

```
src/
  utils/
    utils.h          — Trim, Seek и прочие утилиты
    parse.h          — парсинг целых, float, Date, DateTime из строк
    hash_map.h       — open-addressing hash map (quadratic probing)
  csv/
    csvreader.h/cpp  — чтение CSV (RFC 4180)
    csvwriter.h/cpp  — запись CSV
  schema/
    schema.h         — DataType enum, ColumnSchema, Schema, LoadSchemaCsv
  engine/
    batch/
      batch.h/cpp    — Batch (колоночное хранение в памяти)
    columnar/
      columnar_format.h    — константы формата (magic, version)
      columnar_writer.h/cpp — запись .columnar файлов
      columnar_reader.h/cpp — чтение через mmap + readahead
    exec/
      operator.h     — базовый класс Operator
      exec_batch.h   — ExecBatch (Batch* + selection vector)
      expr.h/cpp     — базовый класс Expr + фабрики
      exprs/         — конкретные выражения (7 типов)
      filter.h       — оператор Filter
      scan.h         — оператор Scan
      project.h      — оператор Project
      hash_aggregate.h/cpp — оператор HashAggregate
      sort.h         — оператор Sort (с LIMIT/OFFSET)
      topk.h         — оператор TopK (heap-based)
      limit.h        — оператор Limit
      func.h/cpp     — реестр скалярных функций
bench/
  clickbench_run.cpp — 43 захардкоженных плана запросов + main
exe/
  csv_to_columnar.cpp — конвертер CSV → columnar
tests/
  test_types.cpp     — тесты типов и парсинга
  test_roundtrip.cpp — тесты columnar read/write roundtrip
  test_exec.cpp      — тесты операторов и выражений
```

---

## Система типов

### DataType (13 типов)

```cpp
enum class DataType {
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    String, Date, DateTime
};
```

**Date** хранится как `int32_t` — количество дней с 1970-01-01.
**DateTime** хранится как `int64_t` — количество секунд с epoch.

### DataVector

```cpp
using DataVector = std::variant<
    std::vector<int8_t>, std::vector<int16_t>, ...,
    std::vector<double>,
    DictColumn
>;
```

Каждая колонка в `Batch` — это один `DataVector`. Числовые колонки хранятся как `vector<T>`, строковые — как `DictColumn` (dictionary encoding в памяти). Тип определяется схемой.

### DictColumn (dictionary-encoded строки)

```cpp
class DictColumn {
    std::deque<std::string> dict_;            // уникальные строки
    std::vector<uint32_t> codes_;             // индекс в dict_ для каждой строки
    std::unordered_map<string_view, uint32_t> index_;  // обратный индекс
};
```

Вместо `vector<string>` (где "iPad" повторяется 1M раз = 1M аллокаций × ~5 байт) — одна копия "iPad" в словаре + 1M × 4 байта codes. Экономия памяти 5-10× на повторяющихся строках.

- `push_back(s)` — O(1) amortized: проверяет index_, добавляет в dict_ если новая
- `operator[](i)` — O(1): `dict_[codes_[i]]`
- `code_at(i)` — O(1): прямой доступ к коду (для hash aggregate)
- `deque` вместо `vector` для dict_ — элементы не перемещаются при росте, string_view в index_ остаются валидны

### EvalType (уровень исполнения)

```cpp
enum class EvalType { I64, U64, F64, Str, Bool, Date, DateTime };
```

При исполнении все узкие типы расширяются:
- `Int8/Int16/Int32/Int64` → `I64` (vector<int64_t>)
- `UInt8/.../UInt64` → `U64` (vector<uint64_t>)
- `Float32/Float64` → `F64` (vector<double>)
- `Date` → `Date` (хранится как int64, но помечен типом Date)
- `DateTime` → `DateTime` (хранится как int64)
- `Bool` → vector<uint8_t> (результат сравнений и логических операций)

### EvalCol

```cpp
using EvalCol = std::variant<
    std::vector<int64_t>, std::vector<uint64_t>,
    std::vector<double>, std::vector<std::string>,
    std::vector<uint8_t>
>;
```

Результат вычисления выражения — один столбец значений.

---

## Колоночный формат (.columnar) — версия 2

### Структура файла

```
┌──────────────────────┐
│ Header (16 bytes)    │  magic "CDB1" + version(2) + footer_offset
├──────────────────────┤
│ Batch 0              │
│   Column 0 data      │  encoding зависит от типа (см. ниже)
│   Column 1 data      │
│   ...                │
├──────────────────────┤
│ Batch 1              │
│   ...                │
├──────────────────────┤
│ Footer               │  schema + batch metadata (offset, size, encoding)
└──────────────────────┘
```

- **Батч** = ~65536 строк. Данные каждой колонки записываются последовательно.
- **Fixed-width колонки** (int8...float64, date, datetime): raw array, encoding = Plain.
- **String колонки**: dictionary encoding (encoding = Dict).

### Encoding: Plain (числовые колонки)

```
[raw bytes: sizeof(T) * nrows]
```

### Encoding: Dict (строковые колонки)

```
┌─────────────────────┐
│ dict_size (uint32)   │  кол-во уникальных строк в батче
├─────────────────────┤
│ len0 (uint32)        │  длина строки 0 в словаре
│ data0 (bytes)        │
│ len1 (uint32)        │
│ data1 (bytes)        │
│ ...                  │
├─────────────────────┤
│ codes (uint32[nrows])│  индекс в словаре для каждой строки
└─────────────────────┘
```

Если в батче 65536 строк, но только 5000 уникальных — вместо хранения 65536 строк целиком хранится 5000 строк + 65536 × 4 байта кодов. На hits.csv это даёт **~35% экономии** размера файла (695 MB → 454 MB на 1M строк).

### Footer (версия 2)

Для каждого chunk в footer хранится:
- `offset: uint64` — позиция в файле
- `size: uint64` — размер в байтах
- `encoding: uint8` — 0 = Plain, 1 = Dict

### Чтение (mmap + readahead)

`ColumnarReader` маппит весь файл через `mmap(PROT_READ, MAP_PRIVATE)`.

- **Plain**: `memcpy` из mapped-региона в `vector<T>`
- **Dict**: читает словарь + `memcpy` кодов → формирует `DictColumn`
- Перед чтением батча N вызывается `readahead(fd, offset, size)` для батча N+1 — ядро асинхронно подгружает страницы с диска

Проекция колонок: если запрос использует 3 из 105 колонок — читаются только они. Остальные колонки не трогаются (seek через mmap offset).

---

## Execution Engine

### Pull-based Volcano Model

Каждый оператор реализует интерфейс:

```cpp
class Operator {
    virtual std::optional<ExecBatch> Next() = 0;
    virtual const Schema& OutputSchema() const = 0;
};
```

Вызывающий код тянет данные вызовом `Next()`. Оператор возвращает один батч за вызов, `nullopt` когда данные кончились.

**ExecBatch** = указатель на Batch + опциональный selection vector:

```cpp
struct ExecBatch {
    const Batch* batch;
    std::vector<uint32_t> sel;  // если пустой — все строки активны
};
```

Selection vector (`sel`) содержит индексы активных строк в батче. Filter заполняет sel, а не копирует данные — zero-copy фильтрация.

### Операторы (7 штук)

Все операторы наследуют `Operator`:

```
Operator (abstract)
  ├── Scan        — читает батчи из ColumnarReader
  ├── Filter      — фильтрует строки по предикату (Bool-выражение)
  ├── Project     — вычисляет новые колонки из выражений
  ├── HashAggregate — GROUP BY + агрегатные функции
  ├── Sort        — полная сортировка (с LIMIT/OFFSET)
  ├── TopK        — top-K через min-heap (для ORDER BY + LIMIT)
  └── Limit       — ограничение числа строк
```

#### Scan

Читает данные из `ColumnarReader`. Три конструктора:

```cpp
Scan(reader)                              // все колонки
Scan(reader, vector<size_t>{0, 3})        // по индексам
Scan(reader, vector<string>{"URL", "UserID"})  // по именам
```

Каждый вызов `Next()` читает один батч. Перед чтением вызывает `Prefetch()` для следующего батча.

#### Filter

Принимает дочерний оператор и предикат (Expr с result_type == Bool).
На каждый батч:
1. Вычисляет предикат → vector<uint8_t> маска
2. Собирает sel из индексов где маска = 1
3. Возвращает ExecBatch с sel (без копирования данных)

Если все строки отфильтрованы — пропускает батч и тянет следующий.

#### Project

Принимает список пар `{имя, выражение}`. На каждый батч вычисляет каждое выражение, формирует новый Batch с новой схемой.

#### HashAggregate

GROUP BY + агрегатные функции. Блокирующий оператор — читает все данные при первом `Next()`.

Два пути исполнения:
- **Fast path** (≤4 ключа, без float): ключи упаковываются в `FastKeyN<N>` (8×N байт). Строковые ключи приходят уже dictionary-encoded из `DictColumn`, их коды (uint32 → int64) кладутся прямо в `FastKeyN`. Hash-таблица: custom `HashMap` с open-addressing и quadratic probing.
- **Generic path** (>4 ключа или float): ключи как `vector<variant<int64,uint64,double,string,uint8>>`, hash-таблица: `std::unordered_map`.

Агрегатные функции (6 видов):

```
GroupAgg (abstract)
  ├── CountStarAgg    — COUNT(*)
  ├── SumAgg<T>       — SUM(expr)
  ├── MinMaxAgg<T>    — MIN/MAX(expr)
  ├── AvgAgg<T>       — AVG(expr) = SUM/COUNT
  └── CountDistinctAgg<T> — COUNT(DISTINCT expr) через unordered_set<T>
```

Каждый агрегат хранит состояние для каждой группы (например `vector<uint64_t> counts_` для CountStar). Метод `UpdateBatch(gids, ctx)` обновляет состояние по вектору group-id для каждой строки батча.

Для скалярных агрегатов (без GROUP BY) используется синтетический ключ `__zero = 0` — все строки попадают в одну группу.

**Runtime Dictionary Encoding (string interning)**:
Строковые ключи GROUP BY не хранятся в hash-таблице напрямую. Вместо этого каждая уникальная строка кладётся в `deque<string>` (string pool) и получает uint32 id. В ключах hash-таблицы хранится id (8 байт) вместо строки (~50 байт). При выдаче результата id разворачивается обратно в строку.

#### Sort

Полная сортировка. Блокирующий оператор — накапливает все батчи в один, сортирует по ключам, применяет LIMIT/OFFSET.

```cpp
Sort(child, keys, limit=MAX, offset=0)
```

Внутри: stable_sort по индексам, gather результата по отсортированным индексам.

#### TopK

Heap-based top-K. Для ORDER BY + LIMIT без OFFSET. Эффективнее Sort когда K << N (не нужно сортировать весь массив).

Для каждой строки: если лучше worst в heap → заменить worst, push_heap. В конце sort heap для финального порядка.

#### Limit

Простой оператор — возвращает первые N строк и останавливается.

### Tiebreakers

Для детерминированных результатов при ORDER BY + LIMIT, все ключи сортировки автоматически дополняются всеми колонками output schema как вторичные ключи (ASC). Это делается в `AppendTiebreakers()`.

---

## Выражения (Expr)

### Базовый класс

```cpp
class Expr {
    virtual EvalType result_type() const = 0;
    virtual EvalCol eval(const EvalContext& ctx) const = 0;
};
```

`EvalContext` = указатель на Batch + указатель на selection vector.

### Иерархия выражений

```
Expr (abstract)
  ├── ColumnExpr     — читает колонку из батча по индексу
  ├── ConstExpr<T>   — возвращает вектор из N одинаковых значений
  ├── CompareExpr    — сравнение двух выражений (Eq, Ne, Lt, Le, Gt, Ge)
  ├── ArithExpr      — арифметика (Add, Sub, Mul, Div, Mod)
  ├── LogicalExpr    — логика (And, Or, Not)
  ├── InListExpr     — IN (value_list)
  ├── IfExpr         — CASE WHEN / IF-THEN-ELSE
  └── FuncCallExpr   — вызов скалярной функции
```

### Фабрики (Factory functions)

Выражения создаются через фабрики, а не напрямую. Это скрывает конкретные классы (они в anonymous namespace) и позволяет добавлять новые типы выражений без изменения заголовков:

```cpp
ExprPtr MakeColumn(const Schema& s, size_t idx);
ExprPtr MakeColumnByName(const Schema& s, string_view name);
ExprPtr MakeConstI64(int64_t v);
ExprPtr MakeConstStr(string v);
ExprPtr MakeCompare(ExprPtr l, CmpOp op, ExprPtr r);
ExprPtr MakeArith(ExprPtr l, ArithOp op, ExprPtr r);
ExprPtr MakeLogical(LogOp op, vector<ExprPtr> args);
ExprPtr MakeInList(ExprPtr lhs, vector<ExprPtr> consts);
ExprPtr MakeIf(ExprPtr cond, ExprPtr if_true, ExprPtr if_false);
ExprPtr MakeFuncCall(const string& name, vector<ExprPtr> args);
```

`ExprPtr = unique_ptr<Expr>` — владение через move-семантику.

### Промотирование типов

При сравнении или арифметике двух разных числовых типов определяется общий тип через `CommonNumericType`:
- Если один F64 → результат F64
- I64 и U64 → F64 (потенциальная потеря точности, но безопасно)
- I64 и I64 → I64
- Date/DateTime считаются как I64

`CastEval<T>` конвертирует EvalCol в vector<T> через static_cast.

### Выражения в файлах

```
src/engine/exec/
  expr.h          — публичный API (EvalType, EvalCol, Expr, фабрики)
  expr.cpp        — реализация фабрик
  exprs/
    helpers.h     — CommonNumericType, CastEval, GatherCast, GatherString
    column.h      — ColumnExpr, ConstExpr<T>
    compare.h     — CompareExpr, CmpLoop, DispatchCmp
    arith.h       — ArithExpr, ArithLoop
    logical.h     — LogicalExpr (And/Or/Not)
    in_list.h     — InListExpr
    if.h          — IfExpr
```

---

## Скалярные функции

### Реестр функций (FuncRegistry)

Singleton с перегрузками по имени и типам аргументов:

```cpp
FuncRegistry::Instance().Lookup("length", {EvalType::Str})
  → ScalarFn { name, arg_types, result_type, impl }
```

### Зарегистрированные функции

| Функция | Аргументы | Результат | Описание |
|---------|-----------|-----------|----------|
| `length` | (Str) | I64 | Длина строки в байтах |
| `like` | (Str, Str) | Bool | SQL LIKE с `%` и `_` |
| `extract` | (Str, DateTime) | I64 | Извлечение year/month/day/hour/minute/second |
| `date_trunc` | (Str, DateTime) | DateTime | Округление до minute/hour/day |
| `regexp_replace` | (Str, Str, Str) | Str | Замена по regex (ECMAScript) |

### LIKE-паттерн

Реализация через итеративный алгоритм с backtracking:
- `%` — любое количество символов
- `_` — ровно один символ
- Остальные — literal match

Оптимизация: если паттерн одинаковый для всех строк батча (всегда при `LIKE '%google%'`), компилируется один раз.

---

## HashMap (custom)

Open-addressing hash map с quadratic probing. Файл: `src/utils/hash_map.h`.

```cpp
template<class Key, class Hash>
class HashMap {
    uint32_t* find(const Key& k);
    void insert(const Key& k, uint32_t v);
    void for_each(Fn fn) const;
    void clear();
};
```

Особенности:
- **Load factor ≤ 50%** — grow при `size * 2 >= capacity`
- **Quadratic probing** — `(h + step*(step+1)/2) & mask` — избегает clustering
- **Power-of-2 размер** — побитовый AND вместо модуля
- **Только insert + find** — нет erase (не нужен для aggregate)
- **Value всегда `uint32_t`** — sentinel `UINT32_MAX` = пустой слот (одна линия памяти на запись)

### Где используется

| Место | Key | Что хранит |
|---|---|---|
| `HashAggregate::ConsumeFastImpl<N>` | `FastKeyN<N>` (≤4 int64) | group id |
| `HashAggregate::Consume()` (generic) | `GroupKey` (variant вектор) | group id |
| `HashAggregate::str_index_` | `string_view` | id в string pool |
| `DictColumn::index_` | `string_view` | id в словаре батча |

### SplitMix64 mixing

Хеш-функция для интегральных ключей использует **SplitMix64** — три раунда `xor-shift + multiply`:

```cpp
v ^= v >> 33;
v *= 0xff51afd7ed558ccdULL;
v ^= v >> 33;
v *= 0xc4ceb9fe1a85ec53ULL;
v ^= v >> 33;
```

Зачем: `std::hash<int64_t>` обычно тождественный — возвращает само число. При power-of-2 размере таблицы и последовательных id (0, 1, 2, ...) все ключи попадают в близкие buckets → кластеризация → O(n²) probe loops.

SplitMix64 даёт идеальный avalanche: изменение 1 бита на входе → ~50% битов на выходе. Это критично когда ключи — id из dictionary encoding (всегда последовательные).

Происхождение: автор Sebastiano Vigna (Java SplittableRandom). Константы — те же что в MurmurHash3 finalizer.

---

## Планы запросов (clickbench_run.cpp)

Запросы не парсятся из SQL. Каждый из 43 запросов ClickBench захардкожен как функция `Plan QN(ColumnarReader&)`.

### DSL для построения планов

```cpp
Plan Q7(ColumnarReader& rdr) {
    Plan p;
    AddScan(p, rdr, {"AdvEngineID"});
    AddFilter(p, Ne(C(p, "AdvEngineID"), MakeConstI64(0)));
    AddHashAgg(p,
        Cols(KV{"AdvEngineID", C(p, "AdvEngineID")}),
        Aggs(CountStar("count")));
    AddSort(p, SortKeys(SK{C(p, "count"), false}));
    AddProject(p, Cols(
        KV{"AdvEngineID", C(p, "AdvEngineID")},
        KV{"count", C(p, "count")}));
    return p;
}
```

- `AddScan/AddFilter/AddHashAgg/AddProject/AddSort/AddTopK` — добавляют оператор в план
- `C(p, "name")` — создаёт ColumnExpr по имени из текущего root
- `Ne/Eq/Ge/Le/Add/Sub/Mul` — создают выражения
- `Aggs(CountStar("c"), Sum("s", expr))` — список агрегатов
- `Cols(KV{"name", expr})` — список колонок для Project

### Структура Plan

```cpp
struct Plan {
    vector<unique_ptr<Operator>> nodes;  // владение операторами
    Operator* root = nullptr;            // текущий верхний оператор
    unordered_map<string, DataType> format_hints;  // для форматирования дат
};
```

`nodes` хранит владение всеми операторами. Каждый `AddOp` пушит оператор и обновляет `root`. Операторы связаны ссылками через `child_`:

```
Scan ← Filter ← HashAggregate ← Sort ← Project
                                          ↑ root
```

### Формат вывода

- Числа: `std::to_string` для целых, `%.15g` для float
- Строки: всегда в кавычках `"..."`
- Date: `YYYY-MM-DD` (через `format_hints`)
- DateTime: `YYYY-MM-DD HH:MM:SS` (через `format_hints`)

---

## CSV-парсер (csv_to_columnar)

Быстрый парсер для конвертации hits.csv → columnar:

- `FILE*` + `getline()` + 4 MB буфер вместо `istream::get()`
- Каждая строка парсится независимо (без multi-line склейки)
- Строки с неправильным числом полей или ошибками парсинга пропускаются с логом
- Батчи по 65536 строк записываются через `ColumnarWriter`

На 99M строк hits.csv: ~20 минут конвертации.

---

## Сборка (CMake)

### Библиотеки

| Цель | Тип | Описание |
|------|-----|----------|
| `utils` | INTERFACE | Утилиты (Trim, Seek, parse, hash_map) |
| `csv` | STATIC | CSV reader/writer |
| `schema` | INTERFACE | DataType, ColumnSchema, LoadSchemaCsv |
| `batch` | STATIC | Batch (колоночное хранение в памяти) |
| `columnar` | STATIC | ColumnarWriter + ColumnarReader (mmap) |
| `exec` | STATIC | Execution engine (операторы, выражения, функции) |

### Исполняемые файлы

| Цель | Описание |
|------|----------|
| `csv_to_columnar` | Конвертер CSV → columnar |
| `clickbench_run` | Исполнитель 43 запросов ClickBench |
| `ColumnarDB` | Legacy CLI (to-csv, to-columnar) |

### Флаги сборки

- **Release**: `-O3`, LTO (`CMAKE_INTERPROCEDURAL_OPTIMIZATION`), `-march=native`
- **Debug**: `-fsanitize=address,undefined`, `-fno-omit-frame-pointer`

---

## Результаты на ClickBench (99M строк, 105 колонок)

43/43 запросов выполняются. 0 FAIL.

Типичные времена (20 GB RAM, 8 vCPU):

| Запрос | Время | Описание |
|--------|-------|----------|
| Q0 | 0.4s | COUNT(*) |
| Q7 | 1.5s | GROUP BY + ORDER BY |
| Q15 | 5.6s | GROUP BY UserID (17M групп) |
| Q18 | 50s | GROUP BY UserID, minute, SearchPhrase |
| Q28 | 242s | REGEXP_REPLACE + HAVING |
| Q32 | 46s | GROUP BY WatchID, ClientIP (~99M групп) |

Сравнение с DuckDB: наш движок в ~10× медленнее (однопоточный, без SIMD, без сжатия).
