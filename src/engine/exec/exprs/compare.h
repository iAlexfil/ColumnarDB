#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "expr.h"
#include "exprs/helpers.h"

namespace exec::detail {

template<class T, class Cmp>
std::vector<std::uint8_t> CmpLoop(const std::vector<T> &l, const std::vector<T> &r, Cmp cmp) {
	std::vector<std::uint8_t> out(l.size());
	for (std::size_t i = 0; i < l.size(); ++i) out[i] = cmp(l[i], r[i]) ? 1 : 0;
	return out;
}

template<class T>
std::vector<std::uint8_t> DispatchCmp(CmpOp op, const std::vector<T> &l, const std::vector<T> &r) {
	switch (op) {
		case CmpOp::Eq: return CmpLoop(l, r, [](const T &a, const T &b) { return a == b; });
		case CmpOp::Ne: return CmpLoop(l, r, [](const T &a, const T &b) { return a != b; });
		case CmpOp::Lt: return CmpLoop(l, r, [](const T &a, const T &b) { return a <  b; });
		case CmpOp::Le: return CmpLoop(l, r, [](const T &a, const T &b) { return a <= b; });
		case CmpOp::Gt: return CmpLoop(l, r, [](const T &a, const T &b) { return a >  b; });
		case CmpOp::Ge: return CmpLoop(l, r, [](const T &a, const T &b) { return a >= b; });
	}
	throw std::runtime_error("DispatchCmp: bad op");
}

class CompareExpr final : public Expr {
public:
	CompareExpr(ExprPtr l, CmpOp op, ExprPtr r) : l_(std::move(l)), op_(op), r_(std::move(r)) {
		if (l_->result_type() == EvalType::Str && r_->result_type() == EvalType::Str) {
			str_ = true;
		} else {
			str_ = false;
			common_ = CommonNumericType(l_->result_type(), r_->result_type());
		}
	}

	EvalType result_type() const override { return EvalType::Bool; }

	EvalCol eval(const EvalContext &ctx) const override {
		EvalCol lc = l_->eval(ctx);
		EvalCol rc = r_->eval(ctx);
		if (str_) {
			return DispatchCmp(op_,
				std::get<std::vector<std::string>>(lc),
				std::get<std::vector<std::string>>(rc));
		}
		switch (common_) {
			case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
				return DispatchCmp(op_, CastEval<std::int64_t>(lc), CastEval<std::int64_t>(rc));
			case EvalType::U64:
				return DispatchCmp(op_, CastEval<std::uint64_t>(lc), CastEval<std::uint64_t>(rc));
			case EvalType::F64:
				return DispatchCmp(op_, CastEval<double>(lc), CastEval<double>(rc));
			default:
				throw std::runtime_error("CompareExpr: bad common type");
		}
	}

private:
	ExprPtr l_, r_;
	CmpOp op_;
	bool str_ = false;
	EvalType common_ = EvalType::I64;
};

}
