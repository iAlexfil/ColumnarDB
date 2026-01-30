#pragma once

#include <charconv>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <system_error>
#include <variant>
#include <vector>
#include <string>
#include <string_view>


namespace utils {
	template<class IsSpace>
	std::string_view Trim(std::string_view s, IsSpace is_space) {
		while (!s.empty() && is_space(static_cast<unsigned char>(s.front()))) {
			s.remove_prefix(1);
		}
		while (!s.empty() && is_space(static_cast<unsigned char>(s.back()))) {
			s.remove_suffix(1);
		}
		return s;
	}

	inline std::string_view Trim(std::string_view s) {
		return Trim(s, [](unsigned char ch) { return std::isspace(ch) != 0; });
	}
}

enum class DataType : uint8_t {
	Int64 = 0,
	String = 1,
};

using DataObject = std::variant<int64_t, std::string>;
using DataVector = std::variant<std::vector<int64_t>, std::vector<std::string> >;

namespace utils {
	inline int64_t ParseInt64(std::string_view s,
	                          std::size_t line_no,
	                          std::string_view col_name) {
		s = Trim(s, [](unsigned char ch) {
			return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
		});
		if (s.empty()) {
			throw std::runtime_error(
				"CSV parse error at line " + std::to_string(line_no) +
				": column '" + std::string(col_name) + "' expects int64, got empty value");
		}

		int64_t value = 0;
		const char *first = s.data();
		const char *last = s.data() + s.size();

		auto [ptr, ec] = std::from_chars(first, last, value, 10);
		if (ec != std::errc{} || ptr != last) {
			throw std::runtime_error(
				"CSV parse error at line " + std::to_string(line_no) +
				": column '" + std::string(col_name) + "' expects int64, got '" + std::string(s) + "'");
		}
		return value;
	}
}


namespace utils {
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
