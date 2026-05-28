#pragma once

#include "expr.h"
#include "exprs/helpers.h"

namespace exec {
	class ColumnExpr final : public Expr {
	public:
		ColumnExpr(std::size_t idx, EvalType result_type)
			: idx_(idx), result_type_(result_type) {
		}

		EvalType result_type() const override { return result_type_; }

		EvalCol eval(const EvalContext &ctx) const override {
			const auto &col = ctx.batch->GetColumn(idx_);
			switch (result_type_) {
				case EvalType::I64:
				case EvalType::Date:
				case EvalType::DateTime: return GatherNumeric<std::int64_t>(col, ctx.sel);
				case EvalType::U64: return GatherNumeric<std::uint64_t>(col, ctx.sel);
				case EvalType::F64: return GatherNumeric<double>(col, ctx.sel);
				case EvalType::Str: return GatherString(col, ctx.sel);
				case EvalType::Bool:
					throw std::runtime_error("expr: bool column read not supported");
			}
			throw std::runtime_error("ColumnExpr: bad result_type");
		}

	private:
		std::size_t idx_;
		EvalType result_type_;
	};

	template<class T>
	class ConstExpr final : public Expr {
	public:
		ConstExpr(T v, EvalType t) : v_(std::move(v)), t_(t) {
		}

		EvalType result_type() const override { return t_; }

		EvalCol eval(const EvalContext &ctx) const override {
			return std::vector<T>(ctx.rows(), v_);
		}

	private:
		T v_;
		EvalType t_;
	};
}
