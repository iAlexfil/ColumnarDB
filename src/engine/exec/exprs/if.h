#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "expr.h"
#include "exprs/helpers.h"

namespace exec::detail {

template<class T>
EvalCol PickIfBranch(const std::vector<std::uint8_t> &cv,
                     const EvalCol &tv, const EvalCol &fv) {
	std::vector<T> tt = CastEval<T>(tv);
	std::vector<T> ff = CastEval<T>(fv);
	std::vector<T> out(cv.size());
	for (std::size_t i = 0; i < cv.size(); ++i) {
		out[i] = cv[i] ? tt[i] : ff[i];
	}
	return out;
}

class IfExpr final : public Expr {
public:
	IfExpr(ExprPtr c, ExprPtr t, ExprPtr f)
		: cond_(std::move(c)), then_(std::move(t)), else_(std::move(f)) {
		if (cond_->result_type() != EvalType::Bool) {
			throw std::runtime_error("IF: condition must be Bool");
		}
		const EvalType tt = then_->result_type();
		const EvalType ft = else_->result_type();
		if (tt == EvalType::Str || ft == EvalType::Str) {
			if (tt != ft) throw std::runtime_error("IF: branch types differ (string vs non-string)");
			result_type_ = EvalType::Str;
		} else {
			result_type_ = CommonNumericType(tt, ft);
		}
	}

	EvalType result_type() const override { return result_type_; }

	EvalCol eval(const EvalContext &ctx) const override {
		auto cv = std::get<std::vector<std::uint8_t>>(cond_->eval(ctx));
		EvalCol tv = then_->eval(ctx);
		EvalCol fv = else_->eval(ctx);

		if (result_type_ == EvalType::Str) {
			const auto &tt = std::get<std::vector<std::string>>(tv);
			const auto &ff = std::get<std::vector<std::string>>(fv);
			std::vector<std::string> out(cv.size());
			for (std::size_t i = 0; i < cv.size(); ++i) {
				out[i] = cv[i] ? tt[i] : ff[i];
			}
			return out;
		}

		switch (result_type_) {
			case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
				return PickIfBranch<std::int64_t>(cv, tv, fv);
			case EvalType::U64: return PickIfBranch<std::uint64_t>(cv, tv, fv);
			case EvalType::F64: return PickIfBranch<double>(cv, tv, fv);
			default: throw std::runtime_error("IfExpr: bad result type");
		}
	}

private:
	ExprPtr cond_, then_, else_;
	EvalType result_type_ = EvalType::I64;
};

}
