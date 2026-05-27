#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expr.h"
#include "operator.h"
#include "schema.h"

namespace exec {
	enum class GroupAggKind { CountStar, Sum, Min, Max, Avg, CountDistinct };

	struct GroupAggSpec {
		std::string name;
		GroupAggKind kind;
		ExprPtr input;
	};

	class GroupAgg;

	class HashAggregate final : public Operator {
	public:
		HashAggregate(Operator &child,
		              std::vector<std::pair<std::string, ExprPtr> > keys,
		              std::vector<GroupAggSpec> aggs);

		~HashAggregate();

		const Schema &OutputSchema() const override { return out_schema_; }

		std::optional<ExecBatch> Next() override;

	private:
		Operator &child_;
		std::vector<std::string> key_names_;
		std::vector<ExprPtr> key_exprs_;
		std::vector<EvalType> key_types_;
		std::vector<std::string> agg_names_;
		std::vector<std::unique_ptr<GroupAgg> > aggs_;
		Schema out_schema_;

		bool consumed_ = false;
		bool use_fast_ = false;
		std::unique_ptr<Batch> result_;

		std::deque<std::string> str_pool_;
		std::unordered_map<std::string_view, std::uint32_t> str_index_;

		std::uint32_t InternString(std::string_view s);

		const std::string &GetInternedString(std::uint32_t id) const { return str_pool_[id]; }

		void Consume();
		void ConsumeFast();
		template<std::size_t N> void ConsumeFastImpl();
	};
}
