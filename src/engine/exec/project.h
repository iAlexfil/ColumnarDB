#pragma once

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "expr.h"
#include "operator.h"
#include "schema.h"

namespace exec {
	class Project final : public Operator {
	public:
		Project(Operator &child, std::vector<std::pair<std::string, ExprPtr> > outputs)
			: child_(child) {
			if (outputs.empty()) throw std::runtime_error("Project: empty output list");
			exprs_.reserve(outputs.size());
			out_schema_.reserve(outputs.size());
			for (auto &[name, expr]: outputs) {
				out_schema_.push_back(ColumnSchema{name, EvalTypeToDataType(expr->result_type())});
				exprs_.push_back(std::move(expr));
			}
		}

		const Schema &OutputSchema() const override { return out_schema_; }

		std::optional<ExecBatch> Next() override {
			auto in = child_.Next();
			if (!in) return std::nullopt;

			EvalContext ctx{in->batch, in->full_selection() ? nullptr : &in->sel};

			buf_ = std::make_unique<Batch>(out_schema_);
			for (std::size_t i = 0; i < exprs_.size(); ++i) {
				EvalCol c = exprs_[i]->eval(ctx);
				auto &slot = buf_->GetColumn(i);
				std::visit([&](auto &&v) {
					using T = std::decay_t<decltype(v)>;
					if constexpr (std::is_same_v<T, std::vector<std::string>>) {
						DictColumn dc;
						for (auto &s : v) dc.push_back(std::move(s));
						slot.emplace<DictColumn>(std::move(dc));
					} else {
						slot.emplace<T>(std::move(v));
					}
				}, c);
			}
			buf_->SetRowCount(ctx.rows());

			ExecBatch out;
			out.batch = buf_.get();
			return out;
		}

	private:
		Operator &child_;
		std::vector<ExprPtr> exprs_;
		Schema out_schema_;
		std::unique_ptr<Batch> buf_;
	};
}
