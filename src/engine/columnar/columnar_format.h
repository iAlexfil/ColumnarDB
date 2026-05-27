#pragma once

#include <cstdint>
#include <vector>

namespace columnar {
	static constexpr std::uint32_t kColumnarVersion = 2;

	enum class Encoding : std::uint8_t {
		Plain = 0,
		Dict  = 1,
	};

	struct ChunkMeta {
		std::uint64_t offset = 0;
		std::uint64_t size = 0;
		Encoding encoding = Encoding::Plain;
	};

	struct BatchMeta {
		std::uint32_t row_count = 0;
		std::vector<ChunkMeta> columns;
	};
}
