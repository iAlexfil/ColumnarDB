#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "batch.h"
#include "columnar_writer.h"
#include "schema.h"

namespace fs = std::filesystem;

namespace {

constexpr std::size_t kBatchRows = 65536;
constexpr std::size_t kReadBuf = 4 * 1024 * 1024;
	
class CsvParser {
public:
	explicit CsvParser(FILE *fp) : fp_(fp) {
		buf_.resize(kReadBuf);
	}

	template<class Fn>
	void ForEachRecord(Fn fn) {
		while (true) {
			std::size_t n = std::fread(buf_.data(), 1, buf_.size(), fp_);
			if (n == 0) break;
			for (std::size_t i = 0; i < n; ++i) Feed(buf_[i], fn);
		}
		if (in_quotes_ || field_started_ || !field_.empty() || !fields_.empty()) {
			FinishField();
			fn(fields_);
			fields_.clear();
		}
	}

private:
	template<class Fn>
	void Feed(char c, Fn &fn) {
		if (in_quotes_) {
			if (pending_quote_) {
				pending_quote_ = false;
				if (c == '"') {
					field_.push_back('"');
					return;
				}
				in_quotes_ = false;
			} else {
				if (c == '"') { pending_quote_ = true; return; }
				field_.push_back(c);
				return;
			}
		}

		if (c == '"' && !field_started_) {
			in_quotes_ = true;
			field_started_ = true;
		} else if (c == ',') {
			FinishField();
		} else if (c == '\n') {
			FinishField();
			fn(fields_);
			fields_.clear();
		} else if (c == '\r') {
		} else {
			field_.push_back(c);
			field_started_ = true;
		}
	}

	void FinishField() {
		fields_.push_back(std::move(field_));
		field_.clear();
		field_started_ = false;
	}

	FILE *fp_;
	std::vector<char> buf_;
	std::vector<std::string> fields_;
	std::string field_;
	bool in_quotes_ = false;
	bool pending_quote_ = false;
	bool field_started_ = false;
};

}

int main(int argc, char **argv) {
	std::string input, schema_path, output;

	for (int i = 1; i < argc; ++i) {
		std::string a = argv[i];
		auto next = [&]() -> std::string {
			if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
			return argv[++i];
		};
		if (a == "--input")       input = next();
		else if (a == "--schema") schema_path = next();
		else if (a == "--output") output = next();
	}

	if (input.empty() || schema_path.empty() || output.empty()) {
		std::cerr << "Usage: csv_to_columnar --input <data.csv> --schema <hits.schema> --output <data.columnar>\n";
		return 1;
	}

	try {
		std::ifstream schema_in(schema_path);
		if (!schema_in) throw std::runtime_error("cannot open schema: " + schema_path);
		Schema schema = LoadSchemaCsv(schema_in);

		FILE *fp = std::fopen(input.c_str(), "rb");
		if (!fp) throw std::runtime_error("cannot open input: " + input);

		if (fs::path(output).has_parent_path()) fs::create_directories(fs::path(output).parent_path());

		columnar::ColumnarWriter writer(output, schema);
		Batch batch(schema);
		batch.Reserve(kBatchRows);

		std::size_t total_rows = 0;
		std::size_t skipped = 0;
		std::size_t line_no = 0;

		CsvParser parser(fp);
		parser.ForEachRecord([&](const std::vector<std::string> &fields) {
			++line_no;
			if (fields.size() != schema.size()) { ++skipped; return; }
			try {
				batch.AppendRow(fields, line_no);
			} catch (const std::exception &) {
				++skipped;
				return;
			}
			if (batch.RowCount() >= kBatchRows) {
				writer.WriteBatch(batch);
				total_rows += batch.RowCount();
				batch.Clear();
			}
		});

		if (batch.RowCount() > 0) {
			writer.WriteBatch(batch);
			total_rows += batch.RowCount();
		}

		std::fclose(fp);
		writer.Finish();

		std::cout << "wrote " << total_rows << " rows to " << output << "\n";
		if (skipped > 0) std::cout << "skipped " << skipped << " malformed rows\n";
		return 0;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
