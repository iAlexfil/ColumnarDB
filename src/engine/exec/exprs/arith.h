#pragma once

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "expr.h"
#include "exprs/helpers.h"
#include "utils/simd.h"

namespace exec {
	template<class T>
	std::vector<T> ArithLoop(ArithOp op, const std::vector<T> &l, const std::vector<T> &r) {
		const std::size_t n = l.size();
		std::vector<T> out(n);
		if constexpr (std::is_same_v<T, std::int64_t>) {
			if (op == ArithOp::Add) {
				simd::AddI64(l.data(), r.data(), out.data(), n);
				return out;
			}
			if (op == ArithOp::Sub) {
				simd::SubI64(l.data(), r.data(), out.data(), n);
				return out;
			}
		}
		if constexpr (std::is_same_v<T, double>) {
			if (op == ArithOp::Add) {
				simd::AddF64(l.data(), r.data(), out.data(), n);
				return out;
			}
			if (op == ArithOp::Sub) {
				simd::SubF64(l.data(), r.data(), out.data(), n);
				return out;
			}
			if (op == ArithOp::Mul) {
				simd::MulF64(l.data(), r.data(), out.data(), n);
				return out;
			}
		}
		switch (op) {
			case ArithOp::Add: for (std::size_t i = 0; i < n; ++i) out[i] = l[i] + r[i];
				break;
			case ArithOp::Sub: for (std::size_t i = 0; i < n; ++i) out[i] = l[i] - r[i];
				break;
			case ArithOp::Mul: for (std::size_t i = 0; i < n; ++i) out[i] = l[i] * r[i];
				break;
			case ArithOp::Div:
				for (std::size_t i = 0; i < n; ++i) {
					if constexpr (std::is_floating_point_v<T>) out[i] = l[i] / r[i];
					else {
						if (r[i] == 0) throw std::runtime_error("arith: divide by zero");
						out[i] = l[i] / r[i];
					}
				}
				break;
			case ArithOp::Mod:
				if constexpr (std::is_integral_v<T>) {
					for (std::size_t i = 0; i < n; ++i) {
						if (r[i] == 0) throw std::runtime_error("arith: mod by zero");
						out[i] = l[i] % r[i];
					}
				} else {
					throw std::runtime_error("arith: mod requires integer");
				}
				break;
		}
		return out;
	}

	class ArithExpr final : public Expr {
	public:
		ArithExpr(ExprPtr l, ArithOp op, ExprPtr r) : l_(std::move(l)), op_(op), r_(std::move(r)) {
			common_ = CommonNumericType(l_->result_type(), r_->result_type());
			if (op_ == ArithOp::Div && common_ != EvalType::F64) common_ = EvalType::F64;
		}

		EvalType result_type() const override { return common_; }

		EvalCol eval(const EvalContext &ctx) const override {
			auto lc = l_->eval(ctx);
			auto rc = r_->eval(ctx);
			switch (common_) {
				case EvalType::I64:
				case EvalType::Date:
				case EvalType::DateTime:
					return ArithLoop(op_, CastEval<std::int64_t>(lc), CastEval<std::int64_t>(rc));
				case EvalType::U64:
					return ArithLoop(op_, CastEval<std::uint64_t>(lc), CastEval<std::uint64_t>(rc));
				case EvalType::F64:
					return ArithLoop(op_, CastEval<double>(lc), CastEval<double>(rc));
				default:
					throw std::runtime_error("ArithExpr: bad common type");
			}
		}

	private:
		ExprPtr l_, r_;
		ArithOp op_;
		EvalType common_ = EvalType::I64;
	};
}
