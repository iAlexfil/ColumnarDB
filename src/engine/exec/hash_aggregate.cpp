#include "hash_aggregate.h"

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <variant>

namespace exec {

using KeyVal = std::variant<std::int64_t, std::uint64_t, double, std::string, std::uint8_t>;

namespace {

struct GroupKey {
	std::vector<KeyVal> v;
	bool operator==(const GroupKey &o) const { return v == o.v; }
};

struct GroupKeyHash {
	std::size_t operator()(const GroupKey &k) const noexcept {
		std::size_t h = 1469598103934665603ull;
		for (const auto &x : k.v) {
			std::size_t hx = std::visit([](const auto &val) -> std::size_t {
				using T = std::decay_t<decltype(val)>;
				if constexpr (std::is_same_v<T, std::string>) return std::hash<std::string>{}(val);
				else return std::hash<T>{}(val);
			}, x);
			h ^= hx + 0x9e3779b97f4a7c15ull + (h << 6) + (h >> 2);
		}
		return h;
	}
};

KeyVal ExtractKey(const EvalCol &col, std::size_t row) {
	return std::visit([row](const auto &v) -> KeyVal { return KeyVal{v[row]}; }, col);
}

void AppendKey(DataVector &slot, const KeyVal &kv) {
	std::visit([&](const auto &x) {
		using T = std::decay_t<decltype(x)>;
		std::get<std::vector<T>>(slot).push_back(x);
	}, kv);
}

}

class GroupAgg {
public:
	virtual ~GroupAgg() = default;
	virtual void EnsureGroups(std::size_t n) = 0;
	virtual void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &ctx) = 0;
	virtual void EmitInto(DataVector &slot) = 0;
	virtual DataType OutputType() const = 0;
};

namespace {

class CountStarAgg : public GroupAgg {
	std::vector<std::uint64_t> counts_;
public:
	void EnsureGroups(std::size_t n) override { counts_.resize(n, 0); }
	void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &) override {
		for (auto g : gids) ++counts_[g];
	}
	void EmitInto(DataVector &slot) override {
		slot.emplace<std::vector<std::uint64_t>>(std::move(counts_));
	}
	DataType OutputType() const override { return DataType::UInt64; }
};

std::unique_ptr<GroupAgg> BuildGroupAgg(GroupAggSpec &spec) {
	switch (spec.kind) {
		case GroupAggKind::CountStar:
			return std::make_unique<CountStarAgg>();
	}
	throw std::runtime_error("BuildGroupAgg: unsupported kind");
}

}

HashAggregate::HashAggregate(Operator &child,
                             std::vector<std::pair<std::string, ExprPtr>> keys,
                             std::vector<GroupAggSpec> aggs)
	: child_(child) {
	if (keys.empty()) throw std::runtime_error("HashAggregate: at least one key required");
	for (auto &[n, e] : keys) {
		key_types_.push_back(e->result_type());
		key_names_.push_back(n);
		key_exprs_.push_back(std::move(e));
	}
	for (auto &s : aggs) {
		agg_names_.push_back(s.name);
		aggs_.push_back(BuildGroupAgg(s));
	}
	out_schema_.reserve(key_names_.size() + agg_names_.size());
	for (std::size_t i = 0; i < key_names_.size(); ++i) {
		out_schema_.push_back(ColumnSchema{key_names_[i], EvalTypeToDataType(key_types_[i])});
	}
	for (std::size_t i = 0; i < agg_names_.size(); ++i) {
		out_schema_.push_back(ColumnSchema{agg_names_[i], aggs_[i]->OutputType()});
	}
}

HashAggregate::~HashAggregate() = default;

void HashAggregate::Consume() {
	std::unordered_map<GroupKey, std::uint32_t, GroupKeyHash> map;
	std::vector<GroupKey> order;
	std::vector<std::uint32_t> gids;

	while (auto eb = child_.Next()) {
		EvalContext ctx{eb->batch, eb->full_selection() ? nullptr : &eb->sel};

		std::vector<EvalCol> key_cols;
		key_cols.reserve(key_exprs_.size());
		for (const auto &e : key_exprs_) key_cols.push_back(e->eval(ctx));

		const std::size_t n = ctx.rows();
		gids.assign(n, 0);
		for (std::size_t r = 0; r < n; ++r) {
			GroupKey k;
			k.v.reserve(key_cols.size());
			for (const auto &kc : key_cols) k.v.push_back(ExtractKey(kc, r));

			auto it = map.find(k);
			std::uint32_t gid;
			if (it == map.end()) {
				gid = static_cast<std::uint32_t>(order.size());
				order.push_back(k);
				map.emplace(std::move(k), gid);
				for (auto &a : aggs_) a->EnsureGroups(order.size());
			} else {
				gid = it->second;
			}
			gids[r] = gid;
		}

		for (auto &a : aggs_) a->UpdateBatch(gids, ctx);
	}

	result_ = std::make_unique<Batch>(out_schema_);
	result_->Reserve(order.size());
	for (const auto &gk : order) {
		for (std::size_t i = 0; i < gk.v.size(); ++i) {
			AppendKey(result_->GetColumn(i), gk.v[i]);
		}
	}
	for (std::size_t i = 0; i < aggs_.size(); ++i) {
		aggs_[i]->EmitInto(result_->GetColumn(key_exprs_.size() + i));
	}
	result_->SetRowCount(order.size());
}

std::optional<ExecBatch> HashAggregate::Next() {
	if (!consumed_) {
		Consume();
		consumed_ = true;
		ExecBatch out;
		out.batch = result_.get();
		return out;
	}
	return std::nullopt;
}

}
