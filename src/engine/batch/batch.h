#pragma once

#include <cstddef>
#include <cstdint>
#include <istream>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "csvreader.h"
#include "schema.h"
#include "utils/utils.h"


class Batch {
public:
	using Column = DataVector;

	explicit Batch(Schema schema);

	void Clear();

	void Reserve(std::size_t rows);

	void AppendRow(const Row &row, std::size_t line_no);

	std::size_t RowCount() const { return row_count_; }
	std::size_t ColCount() const { return columns_.size(); }

	const Schema &GetSchema() const { return schema_; }
	const std::vector<Column> &Columns() const { return columns_; }

	const Column &GetColumn(std::size_t i) const { return columns_[i]; }
	Column &GetColumn(std::size_t i) { return columns_[i]; }

	void SetRowCount(std::size_t n) { row_count_ = n; }

private:
	Schema schema_;
	std::vector<Column> columns_;
	std::size_t row_count_ = 0;
};


class CsvBatchReader {
public:
	CsvBatchReader(std::istream &in,
	               const Schema &schema,
	               std::size_t batch_rows = (1 << 16),
	               char delimiter = ',');

	std::optional<Batch> ReadNext();

	std::size_t CurrentLine() const { return line_no_; }
	std::size_t BatchRows() const { return batch_rows_; }

private:
	CSVReader reader_;
	const Schema &schema_;
	std::size_t batch_rows_;
	std::size_t line_no_ = 0;
	bool eof_ = false;

	static bool IsAllEmpty(const Row &row);
};
