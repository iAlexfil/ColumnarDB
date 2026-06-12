#pragma once

#include <cstddef>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "batch.h"
#include "columnar_reader.h"
#include "operator.h"

namespace exec {
	class Scan final : public Operator {
	public:
		explicit Scan(columnar::ColumnarReader &reader)
			: reader_(reader) {
			projection_.resize(reader_.GetSchema().size());
			std::iota(projection_.begin(), projection_.end(), std::size_t{0});
			out_schema_ = reader_.GetSchema();
		}

		Scan(columnar::ColumnarReader &reader, std::vector<std::size_t> projection)
			: reader_(reader), projection_(std::move(projection)) {
			out_schema_.reserve(projection_.size());
			for (auto i: projection_) {
				out_schema_.push_back(reader_.GetSchema()[i]);
			}
		}

		Scan(columnar::ColumnarReader &reader, const std::vector<std::string> &column_names)
			: reader_(reader) {
			const auto &full = reader_.GetSchema();
			projection_.reserve(column_names.size());
			for (const auto &n: column_names) {
				bool found = false;
				for (std::size_t i = 0; i < full.size(); ++i) {
					if (full[i].name == n) {
						projection_.push_back(i);
						found = true;
						break;
					}
				}
				if (!found) throw std::runtime_error("Scan: column not found: " + n);
			}
			out_schema_.reserve(projection_.size());
			for (auto i: projection_) out_schema_.push_back(full[i]);
		}

		const Schema &OutputSchema() const override { return out_schema_; }

		std::optional<ExecBatch> Next() override {
			if (next_batch_ >= reader_.NumBatches()) return std::nullopt;

			if (next_batch_ + 1 < reader_.NumBatches()) {
				reader_.Prefetch(next_batch_ + 1, projection_);
			}

			current_ = std::make_unique<Batch>(reader_.ReadBatchColumns(next_batch_, projection_));
			++next_batch_;
			ExecBatch e;
			e.batch = current_.get();
			return e;
		}

	private:
		columnar::ColumnarReader &reader_;
		std::vector<std::size_t> projection_;
		Schema out_schema_;
		std::size_t next_batch_ = 0;
		std::unique_ptr<Batch> current_;
	};
}
