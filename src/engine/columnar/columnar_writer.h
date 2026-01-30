#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "schema.h"
#include "columnar_format.h"

class Batch;

namespace columnar {

	class ColumnarWriter {
	public:
		ColumnarWriter(const std::filesystem::path& path, const Schema& schema);
		~ColumnarWriter();

		ColumnarWriter(const ColumnarWriter&) = delete;
		ColumnarWriter& operator=(const ColumnarWriter&) = delete;

		void WriteBatch(const Batch& batch);
		void Finish();

		const Schema& GetSchema() const { return schema_; }

	private:
		std::ofstream out_;
		Schema schema_;
		std::vector<BatchMeta> batches_;
		bool finalized_ = false;

		void WriteHeader();
		void WriteFooter(std::uint64_t footer_offset);
		void PatchFooterOffset(std::uint64_t footer_offset);
	};

}
