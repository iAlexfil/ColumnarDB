#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "batch.h"

namespace exec {
	struct ExecBatch {
		const Batch *batch = nullptr;
		std::vector<std::uint32_t> sel;

		std::size_t size() const {
			return sel.empty() ? (batch ? batch->RowCount() : 0) : sel.size();
		}

		bool full_selection() const { return sel.empty(); }
	};
}
