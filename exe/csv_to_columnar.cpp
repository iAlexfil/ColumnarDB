#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "batch.h"
#include "columnar_writer.h"
#include "schema.h"

namespace fs = std::filesystem;

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

		std::ifstream data_in(input);
		if (!data_in) throw std::runtime_error("cannot open input: " + input);

		if (fs::path(output).has_parent_path()) fs::create_directories(fs::path(output).parent_path());

		CsvBatchReader reader(data_in, schema);
		columnar::ColumnarWriter writer(output, schema);

		std::size_t total_rows = 0;
		while (auto batch = reader.ReadNext()) {
			writer.WriteBatch(*batch);
			total_rows += batch->RowCount();
		}
		writer.Finish();

		std::cout << "wrote " << total_rows << " rows to " << output << "\n";
		return 0;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
