#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

#include "expr.h"
#include "operator.h"

namespace exec {

class Filter final : public Operator {
public:
	Filter(Operator &child, ExprPtr predicate)
		: child_(child), pred_(std::move(predicate)) {
		if (pred_->result_type() != EvalType::Bool) {
			throw std::runtime_error("Filter: predicate must be Bool");
		}
	}

	const Schema &OutputSchema() const override { return child_.OutputSchema(); }

	std::optional<ExecBatch> Next() override {
		while (true) {
			auto in = child_.Next();
			if (!in) return std::nullopt;

			EvalContext ctx{in->batch, in->full_selection() ? nullptr : &in->sel};
			auto mask = std::get<std::vector<std::uint8_t>>(pred_->eval(ctx));

			sel_buf_.clear();
			if (in->full_selection()) {
				for (std::size_t i = 0; i < mask.size(); ++i) {
					if (mask[i]) sel_buf_.push_back(static_cast<std::uint32_t>(i));
				}
			} else {
				for (std::size_t i = 0; i < mask.size(); ++i) {
					if (mask[i]) sel_buf_.push_back(in->sel[i]);
				}
			}
			if (sel_buf_.empty()) continue;

			ExecBatch out;
			out.batch = in->batch;
			out.sel = sel_buf_;
			return out;
		}
	}

private:
	Operator &child_;
	ExprPtr pred_;
	std::vector<std::uint32_t> sel_buf_;
};

}
