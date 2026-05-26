#include <cstdio>
#include <cstring>
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

void SplitCsvLine(const char *line, std::size_t len, std::vector<std::string> &out) {
	out.clear();
	std::string field;
	bool in_quotes = false;
	for (std::size_t i = 0; i < len; ++i) {
		char c = line[i];
		if (in_quotes) {
			if (c == '"') {
				if (i + 1 < len && line[i + 1] == '"') {
					field.push_back('"');
					++i;
				} else {
					in_quotes = false;
				}
			} else {
				field.push_back(c);
			}
		} else {
			if (c == '"') {
				in_quotes = true;
			} else if (c == ',') {
				out.push_back(std::move(field));
				field.clear();
			} else {
				field.push_back(c);
			}
		}
	}
	out.push_back(std::move(field));
}

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

		FILE *fp = std::fopen(input.c_str(), "r");
		if (!fp) throw std::runtime_error("cannot open input: " + input);
		std::setvbuf(fp, nullptr, _IOFBF, kReadBuf);

		if (fs::path(output).has_parent_path()) fs::create_directories(fs::path(output).parent_path());

		columnar::ColumnarWriter writer(output, schema);

		Batch batch(schema);
		batch.Reserve(kBatchRows);

		std::size_t total_rows = 0;
		std::size_t line_no = 0;
		std::vector<std::string> fields;
		char *line_buf = nullptr;
		std::size_t line_cap = 0;

		std::string accum;
		while (true) {
			ssize_t nread = getline(&line_buf, &line_cap, fp);
			if (nread < 0) {
				if (!accum.empty()) {
					SplitCsvLine(accum.data(), accum.size(), fields);
					batch.AppendRow(fields, line_no);
				}
				break;
			}
			++line_no;

			while (nread > 0 && (line_buf[nread - 1] == '\n' || line_buf[nread - 1] == '\r'))
				--nread;

			if (!accum.empty()) {
				accum.push_back('\n');
				accum.append(line_buf, static_cast<std::size_t>(nread));
				std::size_t quotes = 0;
				for (char c : accum) { if (c == '"') ++quotes; }
				if (quotes % 2 != 0) continue;
				SplitCsvLine(accum.data(), accum.size(), fields);
				accum.clear();
			} else {
				if (nread == 0) continue;
				std::size_t quotes = 0;
				for (ssize_t j = 0; j < nread; ++j) { if (line_buf[j] == '"') ++quotes; }
				if (quotes % 2 != 0) {
					accum.assign(line_buf, static_cast<std::size_t>(nread));
					continue;
				}
				SplitCsvLine(line_buf, static_cast<std::size_t>(nread), fields);
			}

			batch.AppendRow(fields, line_no);

			if (batch.RowCount() >= kBatchRows) {
				writer.WriteBatch(batch);
				total_rows += batch.RowCount();
				batch.Clear();
			}
		}

		if (batch.RowCount() > 0) {
			writer.WriteBatch(batch);
			total_rows += batch.RowCount();
		}

		std::free(line_buf);
		std::fclose(fp);
		writer.Finish();

		std::cout << "wrote " << total_rows << " rows to " << output << "\n";
		return 0;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
