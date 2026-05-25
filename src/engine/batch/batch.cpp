#include "batch.h"

#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

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

		std::visit([&](auto &vec) {
			using T = typename std::decay_t<decltype(vec)>::value_type;
			if constexpr (std::is_same_v<T, std::string>) {
				vec.push_back(field);
			} else if constexpr (std::is_floating_point_v<T>) {
				vec.push_back(utils::ParseFloating<T>(field, line_no, cs.name));
			} else if constexpr (std::is_same_v<T, std::int32_t>) {
				if (cs.type == DataType::Date)
					vec.push_back(utils::ParseDate(field, line_no, cs.name));
				else
					vec.push_back(utils::ParseInteger<T>(field, line_no, cs.name));
			} else if constexpr (std::is_same_v<T, std::int64_t>) {
				if (cs.type == DataType::DateTime)
					vec.push_back(utils::ParseDateTime(field, line_no, cs.name));
				else
					vec.push_back(utils::ParseInteger<T>(field, line_no, cs.name));
			} else {
				vec.push_back(utils::ParseInteger<T>(field, line_no, cs.name));
			}
		}, columns_[i]);
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
