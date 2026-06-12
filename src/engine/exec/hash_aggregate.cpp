#include "hash_aggregate.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "utils/hash_map.h"


namespace exec {
	using KeyVal = std::variant<std::int64_t, std::uint64_t, double, std::string, std::uint8_t>;

	namespace {
		constexpr std::size_t kMaxFastKeys = 4;

		struct FastKey {
			std::array<std::int64_t, kMaxFastKeys> data{};

			bool operator==(const FastKey &o) const noexcept {
				return std::memcmp(data.data(), o.data.data(), sizeof(data)) == 0;
			}
		};

		struct FastKeyHash {
			std::size_t operator()(const FastKey &k) const noexcept {
				std::size_t h = 1469598103934665603ULL;
				for (std::int64_t x: k.data) {
					auto v = static_cast<std::uint64_t>(x);
					v ^= v >> 33;
					v *= 0xff51afd7ed558ccdULL;
					v ^= v >> 33;
					v *= 0xc4ceb9fe1a85ec53ULL;
					v ^= v >> 33;
					h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
				}
				return h;
			}
		};


		struct GroupKey {
			std::vector<KeyVal> v;
			bool operator==(const GroupKey &o) const { return v == o.v; }
		};

		struct GroupKeyHash {
			std::size_t operator()(const GroupKey &k) const noexcept {
				std::size_t h = 1469598103934665603ull;
				for (const auto &x: k.v) {
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
				if constexpr (std::is_same_v<T, std::string>) {
					std::get<DictColumn>(slot).push_back(x);
				} else {
					std::get<std::vector<T>>(slot).push_back(x);
				}
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
				for (auto g: gids) ++counts_[g];
			}

			void EmitInto(DataVector &slot) override {
				slot.emplace<std::vector<std::uint64_t> >(std::move(counts_));
			}

			DataType OutputType() const override { return DataType::UInt64; }
		};

		template<class T>
		class SumAgg : public GroupAgg {
			ExprPtr expr_;
			using Acc = std::conditional_t<std::is_floating_point_v<T>, double,
				std::conditional_t<std::is_signed_v<T>, std::int64_t, std::uint64_t> >;
			std::vector<Acc> sums_;

		public:
			explicit SumAgg(ExprPtr e) : expr_(std::move(e)) {
			}

			void EnsureGroups(std::size_t n) override { sums_.resize(n, 0); }

			void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &ctx) override {
				auto col = expr_->eval(ctx);
				const auto &v = std::get<std::vector<T> >(col);
				for (std::size_t i = 0; i < gids.size(); ++i) sums_[gids[i]] += static_cast<Acc>(v[i]);
			}

			void EmitInto(DataVector &slot) override { slot.emplace<std::vector<Acc> >(std::move(sums_)); }

			DataType OutputType() const override {
				if constexpr (std::is_floating_point_v<T>) return DataType::Float64;
				else if constexpr (std::is_signed_v<T>) return DataType::Int64;
				else return DataType::UInt64;
			}
		};

		template<class T>
		class MinMaxAgg : public GroupAgg {
			ExprPtr expr_;
			bool is_min_;
			std::vector<T> values_;
			std::vector<std::uint8_t> has_;

		public:
			MinMaxAgg(ExprPtr e, bool is_min) : expr_(std::move(e)), is_min_(is_min) {
			}

			void EnsureGroups(std::size_t n) override {
				values_.resize(n, T{});
				has_.resize(n, 0);
			}

			void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &ctx) override {
				auto col = expr_->eval(ctx);
				const auto &v = std::get<std::vector<T> >(col);
				for (std::size_t i = 0; i < gids.size(); ++i) {
					auto g = gids[i];
					if (!has_[g]) {
						values_[g] = v[i];
						has_[g] = 1;
						continue;
					}
					if (is_min_ ? (v[i] < values_[g]) : (v[i] > values_[g])) values_[g] = v[i];
				}
			}

			void EmitInto(DataVector &slot) override {
				if constexpr (std::is_same_v<T, std::string>) {
					DictColumn dc;
					for (auto &s : values_) dc.push_back(std::move(s));
					slot.emplace<DictColumn>(std::move(dc));
				} else {
					slot.emplace<std::vector<T>>(std::move(values_));
				}
			}

			DataType OutputType() const override {
				if constexpr (std::is_same_v<T, std::int64_t>) return DataType::Int64;
				if constexpr (std::is_same_v<T, std::uint64_t>) return DataType::UInt64;
				if constexpr (std::is_same_v<T, double>) return DataType::Float64;
				if constexpr (std::is_same_v<T, std::string>) return DataType::String;
				throw std::runtime_error("MinMaxAgg: unsupported type");
			}
		};

		template<class T>
		class AvgAgg : public GroupAgg {
			ExprPtr expr_;
			std::vector<double> sums_;
			std::vector<std::uint64_t> counts_;

		public:
			explicit AvgAgg(ExprPtr e) : expr_(std::move(e)) {
			}

			void EnsureGroups(std::size_t n) override {
				sums_.resize(n, 0);
				counts_.resize(n, 0);
			}

			void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &ctx) override {
				auto col = expr_->eval(ctx);
				const auto &v = std::get<std::vector<T> >(col);
				for (std::size_t i = 0; i < gids.size(); ++i) {
					sums_[gids[i]] += static_cast<double>(v[i]);
					++counts_[gids[i]];
				}
			}

			void EmitInto(DataVector &slot) override {
				std::vector<double> out(sums_.size());
				for (std::size_t i = 0; i < sums_.size(); ++i) {
					out[i] = counts_[i] > 0 ? sums_[i] / static_cast<double>(counts_[i]) : 0.0;
				}
				slot.emplace<std::vector<double> >(std::move(out));
			}

			DataType OutputType() const override { return DataType::Float64; }
		};

		template<class T>
		class CountDistinctAgg : public GroupAgg {
			ExprPtr expr_;
			std::vector<std::unordered_set<T> > sets_;

		public:
			explicit CountDistinctAgg(ExprPtr e) : expr_(std::move(e)) {
			}

			void EnsureGroups(std::size_t n) override { sets_.resize(n); }

			void UpdateBatch(const std::vector<std::uint32_t> &gids, const EvalContext &ctx) override {
				auto col = expr_->eval(ctx);
				const auto &v = std::get<std::vector<T> >(col);
				for (std::size_t i = 0; i < gids.size(); ++i) sets_[gids[i]].insert(v[i]);
			}

			void EmitInto(DataVector &slot) override {
				std::vector<std::uint64_t> out(sets_.size());
				for (std::size_t i = 0; i < sets_.size(); ++i) out[i] = sets_[i].size();
				slot.emplace<std::vector<std::uint64_t> >(std::move(out));
			}

			DataType OutputType() const override { return DataType::UInt64; }
		};

		std::unique_ptr<GroupAgg> BuildGroupAgg(GroupAggSpec &spec) {
			if (spec.kind == GroupAggKind::CountStar) return std::make_unique<CountStarAgg>();
			if (!spec.input) throw std::runtime_error("aggregate: input required");

			const EvalType t = spec.input->result_type();
			ExprPtr e = std::move(spec.input);

			if (spec.kind == GroupAggKind::Sum) {
				switch (t) {
					case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
						return std::make_unique<SumAgg<std::int64_t>>(std::move(e));
					case EvalType::U64: return std::make_unique<SumAgg<std::uint64_t>>(std::move(e));
					case EvalType::F64: return std::make_unique<SumAgg<double>>(std::move(e));
					default: throw std::runtime_error("SUM: numeric input required");
				}
			}
			if (spec.kind == GroupAggKind::Avg) {
				switch (t) {
					case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
						return std::make_unique<AvgAgg<std::int64_t>>(std::move(e));
					case EvalType::U64: return std::make_unique<AvgAgg<std::uint64_t>>(std::move(e));
					case EvalType::F64: return std::make_unique<AvgAgg<double>>(std::move(e));
					default: throw std::runtime_error("AVG: numeric input required");
				}
			}
			if (spec.kind == GroupAggKind::Min || spec.kind == GroupAggKind::Max) {
				const bool is_min = (spec.kind == GroupAggKind::Min);
				switch (t) {
					case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
						return std::make_unique<MinMaxAgg<std::int64_t>>(std::move(e), is_min);
					case EvalType::U64: return std::make_unique<MinMaxAgg<std::uint64_t>>(std::move(e), is_min);
					case EvalType::F64: return std::make_unique<MinMaxAgg<double>>(std::move(e), is_min);
					case EvalType::Str: return std::make_unique<MinMaxAgg<std::string>>(std::move(e), is_min);
					default: throw std::runtime_error("MIN/MAX: unsupported input type");
				}
			}
			if (spec.kind == GroupAggKind::CountDistinct) {
				switch (t) {
					case EvalType::I64: case EvalType::Date: case EvalType::DateTime:
						return std::make_unique<CountDistinctAgg<std::int64_t>>(std::move(e));
					case EvalType::U64: return std::make_unique<CountDistinctAgg<std::uint64_t>>(std::move(e));
					case EvalType::F64: return std::make_unique<CountDistinctAgg<double>>(std::move(e));
					case EvalType::Str: return std::make_unique<CountDistinctAgg<std::string>>(std::move(e));
					default: throw std::runtime_error("COUNT DISTINCT: unsupported input type");
				}
			}
			throw std::runtime_error("BuildGroupAgg: unsupported kind");
		}
	}

	HashAggregate::HashAggregate(Operator &child,
	                             std::vector<std::pair<std::string, ExprPtr> > keys,
	                             std::vector<GroupAggSpec> aggs)
		: child_(child) {
		if (keys.empty()) throw std::runtime_error("HashAggregate: at least one key required");

		out_schema_.reserve(keys.size() + aggs.size());
		for (auto &[name, expr]: keys) {
			key_types_.push_back(expr->result_type());
			out_schema_.push_back(ColumnSchema{name, EvalTypeToDataType(expr->result_type())});
			key_exprs_.push_back(std::move(expr));
		}
		for (auto &spec: aggs) {
			auto agg = BuildGroupAgg(spec);
			out_schema_.push_back(ColumnSchema{spec.name, agg->OutputType()});
			aggs_.push_back(std::move(agg));
		}

		use_fast_ = key_types_.size() <= kMaxFastKeys;
		for (EvalType t: key_types_) {
			if (t == EvalType::F64) { use_fast_ = false; break; }
		}
	}

	HashAggregate::~HashAggregate() = default;

	std::uint32_t HashAggregate::InternString(std::string_view s) {
		auto *p = str_index_.find(s);
		if (p) return *p;
		str_pool_.emplace_back(s);
		std::uint32_t id = static_cast<std::uint32_t>(str_pool_.size() - 1);
		str_index_.insert(std::string_view(str_pool_.back()), id);
		return id;
	}

	void HashAggregate::Consume() {
		HashMap<GroupKey, GroupKeyHash> map;
		std::vector<std::uint32_t> gids;
		std::uint32_t num_groups = 0;

		while (auto eb = child_.Next()) {
			EvalContext ctx{eb->batch, eb->full_selection() ? nullptr : &eb->sel};

			std::vector<EvalCol> key_cols;
			key_cols.reserve(key_exprs_.size());
			for (const auto &e: key_exprs_) key_cols.push_back(e->eval(ctx));

			const std::size_t n = ctx.rows();
			gids.assign(n, 0);
			for (std::size_t r = 0; r < n; ++r) {
				GroupKey k;
				k.v.reserve(key_cols.size());
				for (const auto &kc: key_cols) k.v.push_back(ExtractKey(kc, r));

				auto *ptr = map.find(k);
				std::uint32_t gid;
				if (!ptr) {
					gid = num_groups++;
					map.insert(k, gid);
					for (auto &a: aggs_) a->EnsureGroups(num_groups);
				} else {
					gid = *ptr;
				}
				gids[r] = gid;
			}

			for (auto &a: aggs_) a->UpdateBatch(gids, ctx);
		}

		result_ = std::make_unique<Batch>(out_schema_);
		result_->Reserve(num_groups);

		std::vector<const GroupKey *> sorted_keys(num_groups);
		map.for_each([&](const GroupKey &k, std::uint32_t gid) { sorted_keys[gid] = &k; });

		for (std::uint32_t gid = 0; gid < num_groups; ++gid) {
			const GroupKey &gk = *sorted_keys[gid];
			for (std::size_t i = 0; i < gk.v.size(); ++i) {
				AppendKey(result_->GetColumn(i), gk.v[i]);
			}
		}
		for (std::size_t i = 0; i < aggs_.size(); ++i) {
			aggs_[i]->EmitInto(result_->GetColumn(key_exprs_.size() + i));
		}
		result_->SetRowCount(num_groups);
	}

	std::optional<ExecBatch> HashAggregate::Next() {
		if (!consumed_) {
			if (use_fast_) ConsumeFast();
			else Consume();
			consumed_ = true;
			ExecBatch out;
			out.batch = result_.get();
			return out;
		}
		return std::nullopt;
	}

	void HashAggregate::ConsumeFast() {
		const std::size_t nkeys = key_types_.size();
		HashMap<FastKey, FastKeyHash> map;
		std::vector<std::uint32_t> gids;
		std::uint32_t num_groups = 0;

		while (auto eb = child_.Next()) {
			EvalContext ctx{eb->batch, eb->full_selection() ? nullptr : &eb->sel};

			const std::size_t n = ctx.rows();

			std::vector<std::vector<std::int64_t>> key_data(nkeys);
			for (std::size_t i = 0; i < nkeys; ++i) {
				EvalCol col = key_exprs_[i]->eval(ctx);
				key_data[i].resize(n);
				if (key_types_[i] == EvalType::Str) {
					const auto &v = std::get<std::vector<std::string>>(col);
					for (std::size_t r = 0; r < n; ++r) {
						key_data[i][r] = static_cast<std::int64_t>(InternString(v[r]));
					}
				} else {
					std::visit([&](const auto &v) {
						using T = typename std::decay_t<decltype(v)>::value_type;
						if constexpr (std::is_arithmetic_v<T>) {
							for (std::size_t r = 0; r < n; ++r) {
								key_data[i][r] = static_cast<std::int64_t>(v[r]);
							}
						} else {
							throw std::runtime_error("ConsumeFast: unexpected non-numeric");
						}
					}, col);
				}
			}

			gids.assign(n, 0);
			for (std::size_t r = 0; r < n; ++r) {
				FastKey k;
				for (std::size_t i = 0; i < nkeys; ++i) k.data[i] = key_data[i][r];

				auto *ptr = map.find(k);
				std::uint32_t gid;
				if (!ptr) {
					gid = num_groups++;
					map.insert(k, gid);
					for (auto &a: aggs_) a->EnsureGroups(num_groups);
				} else {
					gid = *ptr;
				}
				gids[r] = gid;
			}

			for (auto &a: aggs_) a->UpdateBatch(gids, ctx);
		}

		result_ = std::make_unique<Batch>(out_schema_);
		result_->Reserve(num_groups);

		std::vector<const FastKey *> sorted_keys(num_groups);
		map.for_each([&](const FastKey &k, std::uint32_t gid) { sorted_keys[gid] = &k; });

		for (std::uint32_t gid = 0; gid < num_groups; ++gid) {
			const FastKey &fk = *sorted_keys[gid];
			for (std::size_t i = 0; i < nkeys; ++i) {
				if (key_types_[i] == EvalType::Str) {
					std::get<DictColumn>(result_->GetColumn(i))
						.push_back(GetInternedString(static_cast<std::uint32_t>(fk.data[i])));
				} else {
					std::visit([v = fk.data[i]](auto &vec) {
						using T = typename std::decay_t<decltype(vec)>::value_type;
						if constexpr (std::is_integral_v<T>) vec.push_back(static_cast<T>(v));
						else throw std::runtime_error("ConsumeFast emit: unexpected non-integer slot");
					}, result_->GetColumn(i));
				}
			}
		}
		for (std::size_t i = 0; i < aggs_.size(); ++i) {
			aggs_[i]->EmitInto(result_->GetColumn(nkeys + i));
		}
		result_->SetRowCount(num_groups);
	}
}
