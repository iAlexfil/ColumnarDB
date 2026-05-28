#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "expr.h"
#include "exprs/helpers.h"

namespace exec {
	template<class T>
	EvalCol EvalInListIntegral(const EvalCol &lc,
	                           const std::vector<ExprPtr> &rhs,
	                           const EvalContext &ctx) {
		std::unordered_set<T> set;
		for (const auto &r: rhs) {
			auto v = CastEval<T>(r->eval(ctx));
			if (!v.empty()) set.insert(v[0]);
		}
		auto lv = CastEval<T>(lc);
		std::vector<std::uint8_t> out(lv.size());
		for (std::size_t i = 0; i < lv.size(); ++i) out[i] = set.count(lv[i]) ? 1 : 0;
		return out;
	}

	class InListExpr final : public Expr {
	public:
		InListExpr(ExprPtr lhs, std::vector<ExprPtr> rhs)
			: lhs_(std::move(lhs)), rhs_(std::move(rhs)) {
			if (rhs_.empty()) throw std::runtime_error("IN: empty list");
			t_ = lhs_->result_type();
			for (const auto &r: rhs_) {
				const EvalType rt = r->result_type();
				if (t_ == EvalType::Str) {
					if (rt != EvalType::Str) throw std::runtime_error("IN: string vs non-string");
				} else if (rt == EvalType::Str || rt == EvalType::Bool) {
					throw std::runtime_error("IN: incompatible types");
				} else {
					t_ = CommonNumericType(t_, rt);
				}
			}
		}

		EvalType result_type() const override { return EvalType::Bool; }

		EvalCol eval(const EvalContext &ctx) const override {
			EvalCol lc = lhs_->eval(ctx);

			if (t_ == EvalType::Str) {
				std::unordered_set<std::string> set;
				for (const auto &r: rhs_) {
					auto rc = std::get<std::vector<std::string> >(r->eval(ctx));
					if (!rc.empty()) set.insert(rc[0]);
				}
				const auto &lv = std::get<std::vector<std::string> >(lc);
				std::vector<std::uint8_t> out(lv.size());
				for (std::size_t i = 0; i < lv.size(); ++i) out[i] = set.count(lv[i]) ? 1 : 0;
				return out;
			}

			switch (t_) {
				case EvalType::I64:
				case EvalType::Date:
				case EvalType::DateTime:
					return EvalInListIntegral<std::int64_t>(lc, rhs_, ctx);
				case EvalType::U64:
					return EvalInListIntegral<std::uint64_t>(lc, rhs_, ctx);
				default:
					throw std::runtime_error("InListExpr: bad type");
			}
		}

	private:
		ExprPtr lhs_;
		std::vector<ExprPtr> rhs_;
		EvalType t_ = EvalType::I64;
	};
}
