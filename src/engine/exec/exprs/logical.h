#pragma once

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "expr.h"

namespace exec::detail {
	class LogicalExpr final : public Expr {
	public:
		LogicalExpr(LogOp op, std::vector<ExprPtr> args) : op_(op), args_(std::move(args)) {
			if (op_ == LogOp::Not) {
				if (args_.size() != 1) throw std::runtime_error("NOT expects 1 arg");
			} else if (args_.size() < 2) {
				throw std::runtime_error("AND/OR expect >=2 args");
			}
			for (auto &a: args_) {
				if (a->result_type() != EvalType::Bool)
					throw std::runtime_error("logical: non-bool arg");
			}
		}

		EvalType result_type() const override { return EvalType::Bool; }

		EvalCol eval(const EvalContext &ctx) const override {
			if (op_ == LogOp::Not) {
				auto v = std::get<std::vector<std::uint8_t> >(args_[0]->eval(ctx));
				for (auto &x: v) x = x ? 0 : 1;
				return v;
			}
			auto acc = std::get<std::vector<std::uint8_t> >(args_[0]->eval(ctx));
			for (std::size_t k = 1; k < args_.size(); ++k) {
				auto next = std::get<std::vector<std::uint8_t> >(args_[k]->eval(ctx));
				for (std::size_t i = 0; i < acc.size(); ++i) {
					acc[i] = (op_ == LogOp::And) ? (acc[i] & next[i]) : (acc[i] | next[i]);
				}
			}
			return acc;
		}

	private:
		LogOp op_;
		std::vector<ExprPtr> args_;
	};
}
