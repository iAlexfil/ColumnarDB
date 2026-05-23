#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <vector>

#include "schema.h"
#include "columnar_format.h"

class Batch;

namespace columnar {

	class ColumnarReader {
	public:
		explicit ColumnarReader(const std::filesystem::path& path);

		const Schema& GetSchema() const { return schema_; }
		std::size_t NumBatches() const { return batches_.size(); }
		const BatchMeta& GetBatchMeta(std::size_t idx) const { return batches_[idx]; }

		Batch ReadBatch(std::size_t idx);
		Batch ReadBatchColumns(std::size_t batch_idx, const std::vector<std::size_t> &col_indices);

	private:
		void ReadOneColumn(std::size_t batch_idx, std::size_t src_col,
		                   DataVector &dst, std::size_t nrows);

		std::ifstream in_;
		Schema schema_;
		std::vector<BatchMeta> batches_;
		std::uint64_t footer_offset_ = 0;

		void ReadHeader();
		void ReadFooter();
	};

}
