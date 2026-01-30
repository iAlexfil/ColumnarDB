#pragma once

#include <cstdint>
#include <vector>

namespace columnar {

	static constexpr std::uint32_t kColumnarVersion = 1;

	struct ChunkMeta {
		std::uint64_t offset = 0;
		std::uint64_t size = 0;
	};

	struct BatchMeta {
		std::uint32_t row_count = 0;
		std::vector<ChunkMeta> columns;
	};

}
