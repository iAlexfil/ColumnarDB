#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "expr.h"

namespace exec::detail {
	inline EvalType CommonNumericType(EvalType a, EvalType b) {
		if (a == EvalType::F64 || b == EvalType::F64) return EvalType::F64;
		if (a == b) return a;
		if (IsIntegerLike(a) && IsIntegerLike(b)) return EvalType::I64;
		if (a == EvalType::U64 && IsIntegerLike(b)) return EvalType::F64;
		if (b == EvalType::U64 && IsIntegerLike(a)) return EvalType::F64;
		throw std::runtime_error("expr: incompatible numeric types");
	}

	template<class T>
	std::vector<T> CastEval(const EvalCol &c) {
		return std::visit([](const auto &v) -> std::vector<T> {
			using S = typename std::decay_t<decltype(v)>::value_type;
			if constexpr (std::is_arithmetic_v<S>) {
				std::vector<T> out(v.size());
				for (std::size_t i = 0; i < v.size(); ++i) out[i] = static_cast<T>(v[i]);
				return out;
			} else {
				throw std::runtime_error("expr: cast non-numeric to numeric");
			}
		}, c);
	}

	template<class Src, class Dst>
	std::vector<Dst> GatherCast(const std::vector<Src> &src,
	                            const std::vector<std::uint32_t> *sel) {
		std::vector<Dst> out;
		if (sel == nullptr) {
			out.resize(src.size());
			for (std::size_t i = 0; i < src.size(); ++i) out[i] = static_cast<Dst>(src[i]);
		} else {
			out.resize(sel->size());
			for (std::size_t i = 0; i < sel->size(); ++i) out[i] = static_cast<Dst>(src[(*sel)[i]]);
		}
		return out;
	}

	template<class Dst>
	EvalCol GatherNumeric(const DataVector &col, const std::vector<std::uint32_t> *sel) {
		return std::visit([&](const auto &v) -> EvalCol {
			using V = std::decay_t<decltype(v)>;
			using SrcT = typename V::value_type;
			if constexpr (std::is_arithmetic_v<SrcT>) {
				return GatherCast<SrcT, Dst>(v, sel);
			} else {
				throw std::runtime_error("expr: arithmetic gather on non-numeric column");
			}
		}, col);
	}

	inline EvalCol GatherString(const DataVector &col, const std::vector<std::uint32_t> *sel) {
		const auto &src = std::get<DictColumn>(col);
		std::vector<std::string> out;
		if (sel == nullptr) {
			out.reserve(src.size());
			for (std::size_t i = 0; i < src.size(); ++i) out.push_back(src[i]);
		} else {
			out.reserve(sel->size());
			for (auto i: *sel) out.push_back(src[i]);
		}
		return out;
	}
}
