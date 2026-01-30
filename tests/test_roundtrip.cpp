// Этот файл с тестами сгенерирован с помощью LLM
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <type_traits>

#include "schema.h"
#include "batch.h"
#include "csvwriter.h"

#include "columnar_reader.h"
#include "columnar_writer.h"

namespace fs = std::filesystem;

// ----------------- helpers -----------------

static fs::path MakeTempDir() {
    const auto base = fs::temp_directory_path();
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    const auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    auto dir = base / ("columnar_tests_" + std::to_string((std::uint64_t)now) + "_" + std::to_string((std::uint64_t)tid));
    fs::create_directories(dir);
    return dir;
}

static void WriteFile(const fs::path& p, const std::string& content) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << "Failed to open file for write: " << p;
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    ASSERT_TRUE(out.good()) << "Failed to write file: " << p;
}

template <class T>
static void WriteObj(std::ofstream& out, const T& v) {
    static_assert(std::is_trivially_copyable_v<T>, "WritePOD requires POD");
    out.write(reinterpret_cast<const char*>(&v), sizeof(T));
    ASSERT_TRUE(out.good());
}

static void WriteBytes(std::ofstream& out, const void* data, std::size_t size) {
    out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    ASSERT_TRUE(out.good());
}

static void WriteString(std::ofstream& out, const std::string& s) {
    ASSERT_LE(s.size(), static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()));
    const std::uint32_t len = static_cast<std::uint32_t>(s.size());
    WriteObj(out, len);
    if (!s.empty()) WriteBytes(out, s.data(), s.size());
}

struct FlatTable {
    Schema schema;
    std::vector<DataVector> columns; // match schema types
    std::size_t rows = 0;
};

static FlatTable FlattenBatches(const Schema& schema, const std::vector<Batch>& batches) {
    FlatTable t;
    t.schema = schema;

    t.columns.reserve(schema.size());
    for (const auto& c : schema) {
        switch (c.type) {
            case DataType::Int64:
                t.columns.emplace_back(std::vector<std::int64_t>{});
                break;
            case DataType::String:
                t.columns.emplace_back(std::vector<std::string>{});
                break;
            default:
                throw std::runtime_error("Unsupported DataType in test");
        }
    }

    for (const auto& b : batches) {
        EXPECT_EQ(b.ColCount(), schema.size());
        t.rows += b.RowCount();

        for (std::size_t c = 0; c < schema.size(); ++c) {
            const auto& col = b.GetColumn(c);
            const auto& cs = schema[c];
            if (cs.type == DataType::Int64) {
                const auto& v = std::get<std::vector<std::int64_t>>(col);
                auto& out = std::get<std::vector<std::int64_t>>(t.columns[c]);
                out.insert(out.end(), v.begin(), v.end());
            } else {
                const auto& v = std::get<std::vector<std::string>>(col);
                auto& out = std::get<std::vector<std::string>>(t.columns[c]);
                out.insert(out.end(), v.begin(), v.end());
            }
        }
    }

    // sanity sizes
    for (std::size_t c = 0; c < schema.size(); ++c) {
        const auto& cs = schema[c];
        if (cs.type == DataType::Int64) {
            EXPECT_EQ(std::get<std::vector<std::int64_t>>(t.columns[c]).size(), t.rows);
        } else {
            EXPECT_EQ(std::get<std::vector<std::string>>(t.columns[c]).size(), t.rows);
        }
    }

    return t;
}

static FlatTable ReadAllFromCsvFiles(const fs::path& schema_path,
                                     const fs::path& data_path,
                                     std::size_t batch_rows) {
    std::ifstream schema_in(schema_path);
    EXPECT_TRUE(schema_in.is_open());
    Schema schema = LoadSchemaCsv(schema_in);

    std::ifstream data_in(data_path);
    EXPECT_TRUE(data_in.is_open());

    CsvBatchReader br(data_in, schema, batch_rows);

    std::vector<Batch> batches;
    while (true) {
        auto b = br.ReadNext();
        if (!b) break;
        batches.push_back(*b);
    }
    return FlattenBatches(schema, batches);
}

static FlatTable ReadAllFromColumnar(const fs::path& col_path) {
    columnar::ColumnarReader reader(col_path);
    Schema schema = reader.GetSchema();

    std::vector<Batch> batches;
    for (std::size_t i = 0; i < reader.NumBatches(); ++i) {
        batches.push_back(reader.ReadBatch(i));
    }
    return FlattenBatches(schema, batches);
}

static void ExpectSchemasEqual(const Schema& a, const Schema& b) {
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].name, b[i].name) << "Schema name mismatch at col " << i;
        EXPECT_EQ(a[i].type, b[i].type) << "Schema type mismatch at col " << i;
    }
}

static void ExpectTablesEqual(const FlatTable& a, const FlatTable& b) {
    ExpectSchemasEqual(a.schema, b.schema);
    ASSERT_EQ(a.rows, b.rows);
    ASSERT_EQ(a.columns.size(), b.columns.size());

    for (std::size_t c = 0; c < a.schema.size(); ++c) {
        if (a.schema[c].type == DataType::Int64) {
            EXPECT_EQ(std::get<std::vector<std::int64_t>>(a.columns[c]),
                      std::get<std::vector<std::int64_t>>(b.columns[c]))
                << "Int64 column mismatch at " << c;
        } else {
            EXPECT_EQ(std::get<std::vector<std::string>>(a.columns[c]),
                      std::get<std::vector<std::string>>(b.columns[c]))
                << "String column mismatch at " << c;
        }
    }
}

static void ExportColumnarToCsv(const fs::path& col_path,
                               const fs::path& out_schema,
                               const fs::path& out_data) {
    columnar::ColumnarReader reader(col_path);
    const Schema& schema = reader.GetSchema();

    {
        std::ofstream out(out_schema, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open());
        SaveSchemaCsv(out, schema);
    }

    std::ofstream out(out_data, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    CSVWriter w(out);

    for (std::size_t rg = 0; rg < reader.NumBatches(); ++rg) {
        Batch b = reader.ReadBatch(rg);
        for (std::size_t r = 0; r < b.RowCount(); ++r) {
            Row row;
            row.resize(b.ColCount());
            for (std::size_t c = 0; c < b.ColCount(); ++c) {
                const auto& cs = schema[c];
                const auto& col = b.GetColumn(c);
                if (cs.type == DataType::Int64) {
                    row[c] = std::to_string(std::get<std::vector<std::int64_t>>(col)[r]);
                } else {
                    row[c] = std::get<std::vector<std::string>>(col)[r];
                }
            }
            ASSERT_TRUE(w.WriteNext(row));
        }
    }
}

// Write columnar from CSV files using CsvBatchReader + ColumnarWriter
static void CsvToColumnar(const fs::path& schema_path,
                          const fs::path& data_path,
                          const fs::path& col_path,
                          std::size_t batch_rows) {
    std::ifstream schema_in(schema_path);
    ASSERT_TRUE(schema_in.is_open());
    Schema schema = LoadSchemaCsv(schema_in);

    std::ifstream data_in(data_path);
    ASSERT_TRUE(data_in.is_open());

    CsvBatchReader br(data_in, schema, batch_rows);
    columnar::ColumnarWriter wr(col_path, schema);
    while (true) {
        auto b = br.ReadNext();
        if (!b) break;
        wr.WriteBatch(*b);
    }
    wr.Finish();
}

// ----------------- round-trip tests -----------------

TEST(ColumnarRoundTrip, MultiRowGroupAndCsvEscaping) {
    const std::string schema_csv =
        "a,int64\n"
        "b,string\n"
        "c,int64\n"
        "note,string\n";

    // Covers:
    // - commas inside quoted field
    // - double quotes inside quoted field (escaped as "")
    // - newline inside quoted field
    // - empty field (,,)
    // - empty string ("")
    const std::string data_csv =
        "1,hello,10,plain\n"
        "2,\"has,comma\",20,\"quote:\"\"x\"\"\"\n"
        "3,\"multi\nline\",30,\"\"\n"
        "4,,40,tail\n";

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";
    const fs::path schema2_path = tmp / "schema2.csv";
    const fs::path data2_path   = tmp / "data2.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 2);
    ASSERT_EQ(expected.rows, 4u);

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 2);

    FlatTable got = ReadAllFromColumnar(col_path);
    ExpectTablesEqual(expected, got);

    ExportColumnarToCsv(col_path, schema2_path, data2_path);
    FlatTable got2 = ReadAllFromCsvFiles(schema2_path, data2_path, /*batch_rows*/ 3);
    ExpectTablesEqual(expected, got2);
}

TEST(ColumnarRoundTrip, SingleRowGroup_Int64BoundariesAndWeirdStrings) {
    const std::string schema_csv =
        "i,int64\n"
        "s,string\n";

    const std::string data_csv =
        "-9223372036854775808,\"\"\n"                    // int64 min, empty string
        "9223372036854775807,\"leading space\"\n"        // int64 max
        "0,\"trailing space \"\n"
        "42,\"tab\tinside\"\n"
        "-7,\"carriage\rreturn\"\n"
        "5,\"mix , \"\"quotes\"\" and comma\"\n";

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";
    const fs::path schema2_path = tmp / "schema2.csv";
    const fs::path data2_path   = tmp / "data2.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 1000);
    ASSERT_EQ(expected.rows, 6u);

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 1000);

    FlatTable got = ReadAllFromColumnar(col_path);
    ExpectTablesEqual(expected, got);

    ExportColumnarToCsv(col_path, schema2_path, data2_path);
    FlatTable got2 = ReadAllFromCsvFiles(schema2_path, data2_path, /*batch_rows*/ 1000);
    ExpectTablesEqual(expected, got2);
}

TEST(ColumnarRoundTrip, CRLFAndNoFinalNewline) {
    const std::string schema_csv =
        "a,int64\r\n"
        "b,string\r\n";

    // CRLF endings + last line has no newline
    const std::string data_csv =
        "1,hello\r\n"
        "2,\"x\r\ny\"\r\n"
        "3,tail"; // no trailing newline

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";
    const fs::path schema2_path = tmp / "schema2.csv";
    const fs::path data2_path   = tmp / "data2.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 2);
    ASSERT_EQ(expected.rows, 3u);

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 2);

    FlatTable got = ReadAllFromColumnar(col_path);
    ExpectTablesEqual(expected, got);

    ExportColumnarToCsv(col_path, schema2_path, data2_path);
    FlatTable got2 = ReadAllFromCsvFiles(schema2_path, data2_path, /*batch_rows*/ 2);
    ExpectTablesEqual(expected, got2);
}

TEST(ColumnarRoundTrip, EmptyDataFileProducesZeroRowGroups) {
    const std::string schema_csv =
        "a,int64\n"
        "b,string\n";

    const std::string data_csv = ""; // empty dataset

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";
    const fs::path schema2_path = tmp / "schema2.csv";
    const fs::path data2_path   = tmp / "data2.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 10);
    ASSERT_EQ(expected.rows, 0u);

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 10);

    columnar::ColumnarReader reader(col_path);
    EXPECT_EQ(reader.NumBatches(), 0u);

    // Export back and ensure it still parses to 0 rows
    ExportColumnarToCsv(col_path, schema2_path, data2_path);
    FlatTable got2 = ReadAllFromCsvFiles(schema2_path, data2_path, /*batch_rows*/ 10);
    ExpectTablesEqual(expected, got2);
}

TEST(ColumnarRoundTrip, SkipsAllEmptyLinesInData) {
    const std::string schema_csv =
        "a,int64\n"
        "b,string\n";

    // CsvBatchReader::IsAllEmpty skips lines where all fields are empty/whitespace
    const std::string data_csv =
        "\n"
        "   ,   \n"      // 2 fields, both whitespace -> should be skipped
        "1,ok\n"
        ",\n"            // 2 empty fields -> should be skipped
        "2,yes\n";

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 1);
    ASSERT_EQ(expected.rows, 2u); // only (1,ok) and (2,yes)

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 1);

    FlatTable got = ReadAllFromColumnar(col_path);
    ExpectTablesEqual(expected, got);
}

TEST(ColumnarRoundTrip, VeryLongStrings) {
    const std::string schema_csv =
        "id,int64\n"
        "payload,string\n";

    std::string big(200000, 'A'); // 200 KB string
    // Quote it to ensure CSVWriter/Reader behavior if commas/newlines were present (here none)
    const std::string data_csv =
        "1," + big + "\n"
        "2," + big + "\n";

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";
    const fs::path col_path    = tmp / "out.columnar";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    FlatTable expected = ReadAllFromCsvFiles(schema_path, data_path, /*batch_rows*/ 1);
    ASSERT_EQ(expected.rows, 2u);

    CsvToColumnar(schema_path, data_path, col_path, /*batch_rows*/ 1);

    FlatTable got = ReadAllFromColumnar(col_path);
    ExpectTablesEqual(expected, got);
}

// ----------------- negative tests (errors) -----------------

TEST(SchemaErrors, DuplicateColumnNameThrows) {
    std::istringstream in(
        "a,int64\n"
        "a,string\n");
    EXPECT_THROW({ (void)LoadSchemaCsv(in); }, std::runtime_error);
}

TEST(SchemaErrors, UnknownTypeThrows) {
    std::istringstream in(
        "a,int64\n"
        "b,unknown_type\n");
    EXPECT_THROW({ (void)LoadSchemaCsv(in); }, std::runtime_error);
}

TEST(CsvBatchReaderErrors, InvalidInt64Throws) {
    const std::string schema_csv = "a,int64\nb,string\n";
    const std::string data_csv   = "not_an_int,x\n";

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    std::ifstream schema_in(schema_path);
    ASSERT_TRUE(schema_in.is_open());
    Schema schema = LoadSchemaCsv(schema_in);

    std::ifstream data_in(data_path);
    ASSERT_TRUE(data_in.is_open());
    CsvBatchReader br(data_in, schema, 10);

    EXPECT_THROW({ (void)br.ReadNext(); }, std::runtime_error);
}

TEST(CsvBatchReaderErrors, WrongFieldCountThrows) {
    const std::string schema_csv = "a,int64\nb,string\n";
    const std::string data_csv   = "1,ok,EXTRA\n"; // 3 fields, expected 2

    auto tmp = MakeTempDir();
    const fs::path schema_path = tmp / "schema.csv";
    const fs::path data_path   = tmp / "data.csv";

    WriteFile(schema_path, schema_csv);
    WriteFile(data_path, data_csv);

    std::ifstream schema_in(schema_path);
    ASSERT_TRUE(schema_in.is_open());
    Schema schema = LoadSchemaCsv(schema_in);

    std::ifstream data_in(data_path);
    ASSERT_TRUE(data_in.is_open());
    CsvBatchReader br(data_in, schema, 10);

    EXPECT_THROW({ (void)br.ReadNext(); }, std::runtime_error);
}

TEST(ColumnarErrors, ReaderRejectsBadMagic) {
    auto tmp = MakeTempDir();
    const fs::path p = tmp / "bad_magic.columnar";

    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());

    const char magic[4] = {'B','A','D','!'};
    WriteBytes(out, magic, 4);
    WriteObj(out, (std::uint32_t)1);
    WriteObj(out, (std::uint64_t)16); // some offset
    out.flush();

    EXPECT_THROW({ columnar::ColumnarReader r(p); }, std::runtime_error);
}

TEST(ColumnarErrors, ReaderRejectsBadVersion) {
    auto tmp = MakeTempDir();
    const fs::path p = tmp / "bad_version.columnar";

    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());

    const char magic[4] = {'C','D','B','1'};
    WriteBytes(out, magic, 4);
    WriteObj(out, (std::uint32_t)999);    // bad version
    WriteObj(out, (std::uint64_t)16);
    out.flush();

    EXPECT_THROW({ columnar::ColumnarReader r(p); }, std::runtime_error);
}

TEST(ColumnarErrors, ReaderRejectsZeroFooterOffset) {
    auto tmp = MakeTempDir();
    const fs::path p = tmp / "zero_footer.columnar";

    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());

    const char magic[4] = {'C','D','B','1'};
    WriteBytes(out, magic, 4);
    WriteObj(out, (std::uint32_t)1);
    WriteObj(out, (std::uint64_t)0); // not finalized
    out.flush();

    EXPECT_THROW({ columnar::ColumnarReader r(p); }, std::runtime_error);
}

TEST(ColumnarErrors, ReaderRejectsChunkOverlapsFooter) {
    auto tmp = MakeTempDir();
    const fs::path p = tmp / "overlap_footer.columnar";

    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());

    // header
    const char magic[4] = {'C','D','B','1'};
    WriteBytes(out, magic, 4);
    WriteObj(out, (std::uint32_t)1);

    // We'll place footer at offset 16 (right after header). Then declare a chunk that overlaps footer.
    const std::uint64_t footer_offset = 16;
    WriteObj(out, footer_offset);

    // footer at 16:
    // ncols=1, col{name="a", type=int64}, nrg=1, rg{row_count=1, chunk_offset=0, chunk_size=100}
    WriteObj(out, (std::uint32_t)1);        // ncols
    WriteString(out, "a");                  // name
    WriteObj(out, (std::uint8_t)0);         // type int64 (DataType::Int64 == 0)
    WriteObj(out, (std::uint32_t)1);        // nrg
    WriteObj(out, (std::uint32_t)1);        // row_count
    WriteObj(out, (std::uint64_t)0);        // chunk_offset
    WriteObj(out, (std::uint64_t)100);      // chunk_size => overlaps footer_offset(16)

    out.flush();

    EXPECT_THROW({ columnar::ColumnarReader r(p); }, std::runtime_error);
}
