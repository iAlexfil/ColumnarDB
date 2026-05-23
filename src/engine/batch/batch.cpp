#include "batch.h"

#include <stdexcept>
#include <string>
#include <string_view>

#include "utils/parse.h"


Batch::Batch(Schema schema)
	: schema_(std::move(schema)) {
	columns_.reserve(schema_.size());
	for (const auto &col : schema_) {
		switch (col.type) {
			case DataType::Int8:     columns_.emplace_back(std::vector<std::int8_t>{}); break;
			case DataType::Int16:    columns_.emplace_back(std::vector<std::int16_t>{}); break;
			case DataType::Int32:    columns_.emplace_back(std::vector<std::int32_t>{}); break;
			case DataType::Int64:    columns_.emplace_back(std::vector<std::int64_t>{}); break;
			case DataType::UInt8:    columns_.emplace_back(std::vector<std::uint8_t>{}); break;
			case DataType::UInt16:   columns_.emplace_back(std::vector<std::uint16_t>{}); break;
			case DataType::UInt32:   columns_.emplace_back(std::vector<std::uint32_t>{}); break;
			case DataType::UInt64:   columns_.emplace_back(std::vector<std::uint64_t>{}); break;
			case DataType::Float32:  columns_.emplace_back(std::vector<float>{}); break;
			case DataType::Float64:  columns_.emplace_back(std::vector<double>{}); break;
			case DataType::String:   columns_.emplace_back(std::vector<std::string>{}); break;
			case DataType::Date:     columns_.emplace_back(std::vector<std::int32_t>{}); break;
			case DataType::DateTime: columns_.emplace_back(std::vector<std::int64_t>{}); break;
			default:
				throw std::runtime_error("Batch: unsupported DataType in schema");
		}
	}
}

void Batch::Clear() {
	for (auto &c : columns_) {
		std::visit([](auto &vec) { vec.clear(); }, c);
	}
	row_count_ = 0;
}

void Batch::Reserve(std::size_t rows) {
	for (auto &c : columns_) {
		std::visit([&](auto &vec) { vec.reserve(rows); }, c);
	}
}

void Batch::AppendRow(const Row &row, std::size_t line_no) {
	if (row.size() != schema_.size()) {
		throw std::runtime_error("CSV parse error");
	}

	for (std::size_t i = 0; i < row.size(); ++i) {
		const auto &cs = schema_[i];
		const std::string &field = row[i];

		switch (cs.type) {
			case DataType::Int8:     std::get<std::vector<std::int8_t>>(columns_[i]).push_back(utils::ParseInteger<std::int8_t>(field, line_no, cs.name)); break;
			case DataType::Int16:    std::get<std::vector<std::int16_t>>(columns_[i]).push_back(utils::ParseInteger<std::int16_t>(field, line_no, cs.name)); break;
			case DataType::Int32:    std::get<std::vector<std::int32_t>>(columns_[i]).push_back(utils::ParseInteger<std::int32_t>(field, line_no, cs.name)); break;
			case DataType::Int64:    std::get<std::vector<std::int64_t>>(columns_[i]).push_back(utils::ParseInteger<std::int64_t>(field, line_no, cs.name)); break;
			case DataType::UInt8:    std::get<std::vector<std::uint8_t>>(columns_[i]).push_back(utils::ParseInteger<std::uint8_t>(field, line_no, cs.name)); break;
			case DataType::UInt16:   std::get<std::vector<std::uint16_t>>(columns_[i]).push_back(utils::ParseInteger<std::uint16_t>(field, line_no, cs.name)); break;
			case DataType::UInt32:   std::get<std::vector<std::uint32_t>>(columns_[i]).push_back(utils::ParseInteger<std::uint32_t>(field, line_no, cs.name)); break;
			case DataType::UInt64:   std::get<std::vector<std::uint64_t>>(columns_[i]).push_back(utils::ParseInteger<std::uint64_t>(field, line_no, cs.name)); break;
			case DataType::Float32:  std::get<std::vector<float>>(columns_[i]).push_back(utils::ParseFloating<float>(field, line_no, cs.name)); break;
			case DataType::Float64:  std::get<std::vector<double>>(columns_[i]).push_back(utils::ParseFloating<double>(field, line_no, cs.name)); break;
			case DataType::String:   std::get<std::vector<std::string>>(columns_[i]).push_back(field); break;
			case DataType::Date:     std::get<std::vector<std::int32_t>>(columns_[i]).push_back(utils::ParseDate(field, line_no, cs.name)); break;
			case DataType::DateTime: std::get<std::vector<std::int64_t>>(columns_[i]).push_back(utils::ParseDateTime(field, line_no, cs.name)); break;
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
	for (const auto &f : row) {
		if (!utils::Trim(f).empty()) return false;
	}
	return true;
}

std::optional<Batch> CsvBatchReader::ReadNext() {
	if (eof_) return std::nullopt;

	Batch batch(schema_);
	batch.Reserve(batch_rows_);

	while (batch.RowCount() < batch_rows_) {
		auto row_opt = reader_.ReadNext();
		if (!row_opt.has_value()) { eof_ = true; break; }
		++line_no_;
		if (IsAllEmpty(*row_opt)) continue;
		batch.AppendRow(*row_opt, line_no_);
	}

	if (batch.RowCount() == 0 && eof_) return std::nullopt;
	return batch;
}
