#include "batch.h"

#include <stdexcept>
#include <string>
#include <string_view>


Batch::Batch(Schema schema)
	: schema_(std::move(schema)) {
	columns_.reserve(schema_.size());
	for (const auto &col: schema_) {
		switch (col.type) {
			case DataType::Int64:
				columns_.emplace_back(std::vector<int64_t>{});
				break;
			case DataType::String:
				columns_.emplace_back(std::vector<std::string>{});
				break;
			default:
				throw std::runtime_error("Batch: unsupported DataType in schema");
		}
	}
}

void Batch::Clear() {
	for (auto &c: columns_) {
		std::visit([](auto &vec) { vec.clear(); }, c);
	}
	row_count_ = 0;
}

void Batch::Reserve(std::size_t rows) {
	for (auto &c: columns_) {
		std::visit([&](auto &vec) { vec.reserve(rows); }, c);
	}
}

void Batch::AppendRow(const Row &row, std::size_t line_no) {
	if (row.size() != schema_.size()) {
		throw std::runtime_error("CSV parse error");
	}

	for (std::size_t i = 0; i < row.size(); ++i) {
		const auto &col_schema = schema_[i];
		const std::string &field = row[i];

		switch (col_schema.type) {
			case DataType::Int64: {
				auto &vec = std::get<std::vector<int64_t> >(columns_[i]);
				vec.push_back(utils::ParseInt64(field, line_no, col_schema.name));
				break;
			}
			case DataType::String: {
				auto &vec = std::get<std::vector<std::string> >(columns_[i]);
				vec.push_back(field);
				break;
			}
			default:
				throw std::runtime_error("unsupported DataType in schema");
		}
	}

	++row_count_;
}


CsvBatchReader::CsvBatchReader(std::istream &in, const Schema &schema, std::size_t batch_rows, char delimiter)
	: reader_(in, delimiter), schema_(schema), batch_rows_(batch_rows) {
	if (schema_.empty()) {
		throw std::runtime_error("CsvBatchReader: schema is empty");
	}
}

bool CsvBatchReader::IsAllEmpty(const Row &row) {
	for (const auto &f: row) {
		if (!utils::Trim(f).empty()) return false;
	}
	return true;
}

std::optional<Batch> CsvBatchReader::ReadNext() {
	if (eof_) {
		return std::nullopt;
	}

	Batch batch(schema_);
	batch.Reserve(batch_rows_);

	while (batch.RowCount() < batch_rows_) {
		auto row_opt = reader_.ReadNext();
		if (!row_opt.has_value()) {
			eof_ = true;
			break;
		}

		++line_no_;
		Row &row = *row_opt;

		if (IsAllEmpty(row)) {
			continue;
		}

		batch.AppendRow(row, line_no_);
	}

	if (batch.RowCount() == 0 && eof_) {
		return std::nullopt;
	}

	return batch;
}
