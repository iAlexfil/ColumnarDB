#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expr.h"
#include "operator.h"
#include "schema.h"

namespace exec {

enum class GroupAggKind { CountStar };

struct GroupAggSpec {
	std::string name;
	GroupAggKind kind;
	ExprPtr input;
};

class GroupAgg;

class HashAggregate final : public Operator {
public:
	HashAggregate(Operator &child,
	              std::vector<std::pair<std::string, ExprPtr>> keys,
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
	std::vector<std::unique_ptr<GroupAgg>> aggs_;
	Schema out_schema_;

	bool consumed_ = false;
	std::unique_ptr<Batch> result_;

	void Consume();
};

}
