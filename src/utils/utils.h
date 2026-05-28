#pragma once

#include <cctype>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "engine/batch/dict_column.h"

enum class DataType : uint8_t {
	Int64 = 0,
	String = 1,
	Int8 = 2,
	Int16 = 3,
	Int32 = 4,
	UInt8 = 5,
	UInt16 = 6,
	UInt32 = 7,
	UInt64 = 8,
	Float32 = 9,
	Float64 = 10,
	Date = 11,
	DateTime = 12,
};

using DataObject = std::variant<
	std::int8_t, std::int16_t, std::int32_t, std::int64_t,
	std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t,
	float, double,
	std::string
>;

using DataVector = std::variant<
	std::vector<std::int8_t>, std::vector<std::int16_t>,
	std::vector<std::int32_t>, std::vector<std::int64_t>,
	std::vector<std::uint8_t>, std::vector<std::uint16_t>,
	std::vector<std::uint32_t>, std::vector<std::uint64_t>,
	std::vector<float>, std::vector<double>,
	DictColumn
>;

namespace utils {
	template<class IsSpace>
	std::string_view Trim(std::string_view s, IsSpace is_space) {
		while (!s.empty() && is_space(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
		while (!s.empty() && is_space(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
		return s;
	}

	inline std::string_view Trim(std::string_view s) {
		return Trim(s, [](unsigned char ch) { return std::isspace(ch) != 0; });
	}

	inline void Seek(std::ifstream &in, std::uint64_t pos) {
		in.clear();
		in.seekg(static_cast<std::streamoff>(pos), std::ios::beg);
		if (!in) throw std::runtime_error("seekg failed");
	}

	inline void Seek(std::ofstream &out, std::uint64_t pos) {
		out.clear();
		out.seekp(static_cast<std::streamoff>(pos), std::ios::beg);
		if (!out) throw std::runtime_error("seekp failed");
	}
}
