#pragma once

#include <charconv>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include "utils/utils.h"

namespace utils {
	inline void ThrowParse(std::size_t line_no,
	                       std::string_view col_name,
	                       std::string_view expected,
	                       std::string_view got) {
		throw std::runtime_error(
			"CSV parse error at line " + std::to_string(line_no) +
			": column '" + std::string(col_name) + "' expects " + std::string(expected) +
			", got '" + std::string(got) + "'");
	}

	inline std::string_view TrimSpaces(std::string_view s) {
		return Trim(s, [](unsigned char ch) {
			return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
		});
	}

	template<class T>
	T ParseInteger(std::string_view s, std::size_t line_no, std::string_view col_name) {
		s = TrimSpaces(s);
		if (s.empty()) ThrowParse(line_no, col_name, "integer", "");
		T value{};
		const char *first = s.data();
		const char *last = s.data() + s.size();
		auto [ptr, ec] = std::from_chars(first, last, value, 10);
		if (ec != std::errc{} || ptr != last) {
			ThrowParse(line_no, col_name, "integer", s);
		}
		return value;
	}

	template<class T>
	T ParseFloating(std::string_view s, std::size_t line_no, std::string_view col_name) {
		s = TrimSpaces(s);
		if (s.empty()) ThrowParse(line_no, col_name, "float", "");
		T value{};
		const char *first = s.data();
		const char *last = s.data() + s.size();
		auto [ptr, ec] = std::from_chars(first, last, value);
		if (ec != std::errc{} || ptr != last) {
			ThrowParse(line_no, col_name, "float", s);
		}
		return value;
	}

	inline int ReadTwoDigits(const char *p) {
		const unsigned d0 = static_cast<unsigned>(p[0]) - '0';
		const unsigned d1 = static_cast<unsigned>(p[1]) - '0';
		if (d0 > 9 || d1 > 9) return -1;
		return static_cast<int>(d0 * 10 + d1);
	}

	inline int ReadFourDigits(const char *p) {
		int v = 0;
		for (int i = 0; i < 4; ++i) {
			const unsigned d = static_cast<unsigned>(p[i]) - '0';
			if (d > 9) return -1;
			v = v * 10 + static_cast<int>(d);
		}
		return v;
	}

	inline std::int32_t ParseDate(std::string_view s, std::size_t line_no, std::string_view col_name) {
		s = TrimSpaces(s);
		if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
			ThrowParse(line_no, col_name, "date YYYY-MM-DD", s);
		}
		const int y = ReadFourDigits(s.data());
		const int m = ReadTwoDigits(s.data() + 5);
		const int d = ReadTwoDigits(s.data() + 8);
		if (y < 0 || m < 0 || d < 0) ThrowParse(line_no, col_name, "date YYYY-MM-DD", s);
		const std::chrono::year_month_day ymd{
			std::chrono::year{y}, std::chrono::month{static_cast<unsigned>(m)},
			std::chrono::day{static_cast<unsigned>(d)}
		};
		if (!ymd.ok()) ThrowParse(line_no, col_name, "valid calendar date", s);
		const std::chrono::sys_days sd{ymd};
		return static_cast<std::int32_t>(sd.time_since_epoch().count());
	}

	inline std::int64_t ParseDateTime(std::string_view s, std::size_t line_no, std::string_view col_name) {
		s = TrimSpaces(s);
		if (s.size() != 19 || s[4] != '-' || s[7] != '-' || s[10] != ' '
		    || s[13] != ':' || s[16] != ':') {
			ThrowParse(line_no, col_name, "datetime YYYY-MM-DD HH:MM:SS", s);
		}
		const int y = ReadFourDigits(s.data());
		const int mo = ReadTwoDigits(s.data() + 5);
		const int d = ReadTwoDigits(s.data() + 8);
		const int h = ReadTwoDigits(s.data() + 11);
		const int mi = ReadTwoDigits(s.data() + 14);
		const int se = ReadTwoDigits(s.data() + 17);
		if ((y | mo | d | h | mi | se) < 0) {
			ThrowParse(line_no, col_name, "datetime YYYY-MM-DD HH:MM:SS", s);
		}
		const std::chrono::year_month_day ymd{
			std::chrono::year{y}, std::chrono::month{static_cast<unsigned>(mo)},
			std::chrono::day{static_cast<unsigned>(d)}
		};
		if (!ymd.ok() || h > 23 || mi > 59 || se > 60) {
			ThrowParse(line_no, col_name, "valid datetime", s);
		}
		const std::chrono::sys_days sd{ymd};
		const std::int64_t day_sec = static_cast<std::int64_t>(sd.time_since_epoch().count()) * 86400LL;
		return day_sec + h * 3600LL + mi * 60LL + se;
	}

	inline constexpr std::size_t kDateBufSize = 10;
	inline constexpr std::size_t kDateTimeBufSize = 19;

	inline void Write2(char *p, int v) {
		p[0] = static_cast<char>('0' + (v / 10) % 10);
		p[1] = static_cast<char>('0' + v % 10);
	}

	inline void Write4(char *p, int v) {
		p[0] = static_cast<char>('0' + (v / 1000) % 10);
		p[1] = static_cast<char>('0' + (v / 100) % 10);
		p[2] = static_cast<char>('0' + (v / 10) % 10);
		p[3] = static_cast<char>('0' + v % 10);
	}

	inline std::size_t FormatDate(std::int32_t days, char *out) {
		const std::chrono::sys_days sd{std::chrono::days{days}};
		const std::chrono::year_month_day ymd{sd};
		Write4(out, static_cast<int>(ymd.year()));
		out[4] = '-';
		Write2(out + 5, static_cast<int>(static_cast<unsigned>(ymd.month())));
		out[7] = '-';
		Write2(out + 8, static_cast<int>(static_cast<unsigned>(ymd.day())));
		return kDateBufSize;
	}

	inline std::size_t FormatDateTime(std::int64_t secs, char *out) {
		const std::int64_t day = (secs >= 0) ? (secs / 86400) : -((-secs + 86399) / 86400);
		const std::int64_t tod = secs - day * 86400;
		const std::chrono::sys_days sd{std::chrono::days{static_cast<int>(day)}};
		const std::chrono::year_month_day ymd{sd};
		Write4(out, static_cast<int>(ymd.year()));
		out[4] = '-';
		Write2(out + 5, static_cast<int>(static_cast<unsigned>(ymd.month())));
		out[7] = '-';
		Write2(out + 8, static_cast<int>(static_cast<unsigned>(ymd.day())));
		out[10] = ' ';
		Write2(out + 11, static_cast<int>(tod / 3600));
		out[13] = ':';
		Write2(out + 14, static_cast<int>((tod / 60) % 60));
		out[16] = ':';
		Write2(out + 17, static_cast<int>(tod % 60));
		return kDateTimeBufSize;
	}
}
