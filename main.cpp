#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "batch.h"
#include "csvwriter.h"
#include "schema.h"
#include "engine/columnar/columnar_reader.h"
#include "engine/columnar/columnar_writer.h"

void PrintUsage(const char *prog) {
	std::cerr
			<< "Usage:\n"
			<< "  " << prog << " to-columnar <schema.csv> <data.csv> <out.columnar>\n"
			<< "  " << prog << " to-csv <in.columnar> <out_schema.csv> <out_data.csv>\n";
}


int ToColumnar(const std::filesystem::path &schema_path,
               const std::filesystem::path &data_path,
               const std::filesystem::path &out_path) {
	std::ifstream schema_in(schema_path);
	if (!schema_in.is_open()) {
		throw std::runtime_error("failed to open schema.csv: " + schema_path.string());
	}
	Schema schema = LoadSchemaCsv(schema_in);

	std::ifstream data_in(data_path);
	if (!data_in.is_open()) {
		throw std::runtime_error("failed to open data.csv: " + data_path.string());
	}

	CsvBatchReader batch_reader(data_in, schema);
	columnar::ColumnarWriter writer(out_path, schema);

	std::size_t total_rows = 0;
	std::size_t groups = 0;
	while (true) {
		auto batch_opt = batch_reader.ReadNext();
		if (!batch_opt.has_value()) break;
		writer.WriteBatch(*batch_opt);
		total_rows += batch_opt->RowCount();
		++groups;
	}
	writer.Finish();
	return 0;
}

int ToCsv(const std::filesystem::path &in_path,
          const std::filesystem::path &out_schema_path,
          const std::filesystem::path &out_data_path) {
	columnar::ColumnarReader reader(in_path);
	const Schema &schema = reader.GetSchema();

	{
		std::ofstream schema_out(out_schema_path);
		if (!schema_out.is_open()) {
			throw std::runtime_error("failed to open output schema.csv: " + out_schema_path.string());
		}
		SaveSchemaCsv(schema_out, schema);
	}

	std::ofstream data_out(out_data_path);
	if (!data_out.is_open()) {
		throw std::runtime_error("failed to open output data.csv: " + out_data_path.string());
	}
	CSVWriter csv_writer(data_out);

	for (std::size_t rg = 0; rg < reader.NumBatches(); ++rg) {
		Batch batch = reader.ReadBatch(rg);
		const std::size_t rows = batch.RowCount();
		const std::size_t cols = batch.ColCount();

		for (std::size_t r = 0; r < rows; ++r) {
			Row out_row;
			out_row.resize(cols);
			for (std::size_t c = 0; c < cols; ++c) {
				const auto &cs = schema[c];
				const auto &col = batch.GetColumn(c);
				switch (cs.type) {
					case DataType::Int64: {
						const auto &vec = std::get<std::vector<std::int64_t> >(col);
						out_row[c] = std::to_string(vec[r]);
						break;
					}
					case DataType::String: {
						const auto &vec = std::get<std::vector<std::string> >(col);
						out_row[c] = vec[r];
						break;
					}
				}
			}
			if (!csv_writer.WriteNext(out_row)) {
				throw std::runtime_error("failed to write data.csv");
			}
		}
	}
	return 0;
}


int main(int argc, char **argv) {
	try {
		if (argc < 2) {
			PrintUsage(argv[0]);
			return 1;
		}

		const std::string mode = argv[1];
		if (argc != 5) {
			PrintUsage(argv[0]);
			return 1;
		}
		if (mode == "to-columnar") {
			return ToColumnar(argv[2], argv[3], argv[4]);
		}

		if (mode == "to-csv") {
			return ToCsv(argv[2], argv[3], argv[4]);
		}

		PrintUsage(argv[0]);
		return 1;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
