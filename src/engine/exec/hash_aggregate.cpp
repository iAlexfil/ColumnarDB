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

		template<std::size_t N>
		struct FastKeyN {
			std::array<std::int64_t, N> data{};

			bool operator==(const FastKeyN &o) const noexcept {
				return std::memcmp(data.data(), o.data.data(), sizeof(data)) == 0;
			}
		};

		template<std::size_t N>
		struct FastKeyNHash {
			std::size_t operator()(const FastKeyN<N> &k) const noexcept {
				std::size_t h = 1469598103934665603ULL;
				for (std::size_t i = 0; i < N; ++i) {
					auto v = static_cast<std::uint64_t>(k.data[i]);
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

		template<class NumericFactory>
		std::unique_ptr<GroupAgg> BuildNumericAgg(EvalType t, NumericFactory factory, const char *name) {
			switch (t) {
				case EvalType::I64:
				case EvalType::Date:
				case EvalType::DateTime:
					return factory.template operator()<std::int64_t>();
				case EvalType::U64: return factory.template operator()<std::uint64_t>();
				case EvalType::F64: return factory.template operator()<double>();
				default: throw std::runtime_error(std::string(name) + ": numeric input required");
			}
		}

		std::unique_ptr<GroupAgg> BuildGroupAgg(GroupAggSpec &spec) {
			if (spec.kind == GroupAggKind::CountStar) return std::make_unique<CountStarAgg>();
			if (!spec.input) throw std::runtime_error("aggregate: input required");

			auto e_type = spec.input->result_type();
			auto e = std::move(spec.input);

			switch (spec.kind) {
				case GroupAggKind::Sum:
					return BuildNumericAgg(e_type,
					                       [&]<class T>() {
						                       return std::make_unique<SumAgg<T> >(std::move(e));
					                       }, "SUM");
				case GroupAggKind::Avg:
					return BuildNumericAgg(e_type,
					                       [&]<class T>() {
						                       return std::make_unique<AvgAgg<T> >(std::move(e));
					                       }, "AVG");
				case GroupAggKind::Min:
				case GroupAggKind::Max: {
					const bool is_min = (spec.kind == GroupAggKind::Min);
					if (e_type == EvalType::Str)
						return std::make_unique<MinMaxAgg<std::string> >(std::move(e), is_min);
					return BuildNumericAgg(e_type,
					                       [&]<class T>() {
						                       return std::make_unique<MinMaxAgg<T> >(std::move(e), is_min);
					                       },
					                       is_min ? "MIN" : "MAX");
				}
				case GroupAggKind::CountDistinct:
					if (e_type == EvalType::Str)
						return std::make_unique<CountDistinctAgg<std::string> >(std::move(e));
					return BuildNumericAgg(e_type,
					                       [&]<class T>() {
						                       return std::make_unique<CountDistinctAgg<T> >(std::move(e));
					                       },
					                       "COUNT DISTINCT");
				case GroupAggKind::CountStar: break;
			}
			throw std::runtime_error("BuildGroupAgg: unsupported kind");
		}
	}

	HashAggregate::HashAggregate(Operator &child,
	                             std::vector<std::pair<std::string, ExprPtr> > keys,
	                             std::vector<GroupAggSpec> aggs)
		: child_(child) {
		if (keys.empty()) throw std::runtime_error("HashAggregate: at least one key required");
		for (auto &[n, e]: keys) {
			key_types_.push_back(e->result_type());
			key_names_.push_back(n);
			key_exprs_.push_back(std::move(e));
		}
		for (auto &s: aggs) {
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

		use_fast_ = key_types_.size() <= kMaxFastKeys;
		for (EvalType t: key_types_) {
			if (t == EvalType::F64) { use_fast_ = false; break; }
		}
	}

	HashAggregate::~HashAggregate() = default;

	std::uint32_t HashAggregate::InternString(std::string_view s) {
		auto it = str_index_.find(s);
		if (it != str_index_.end()) return it->second;
		str_pool_.emplace_back(s);
		std::uint32_t id = static_cast<std::uint32_t>(str_pool_.size() - 1);
		str_index_.emplace(std::string_view(str_pool_.back()), id);
		return id;
	}

	void HashAggregate::Consume() {
		std::unordered_map<GroupKey, std::uint32_t, GroupKeyHash> map;
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

				auto it = map.find(k);
				std::uint32_t gid;
				if (it == map.end()) {
					gid = num_groups++;
					map.insert({std::move(k), gid});
					for (auto &a: aggs_) a->EnsureGroups(num_groups);
				} else {
					gid = it->second;
				}
				gids[r] = gid;
			}

			for (auto &a: aggs_) a->UpdateBatch(gids, ctx);
		}

		result_ = std::make_unique<Batch>(out_schema_);
		result_->Reserve(num_groups);

		std::vector<const GroupKey *> sorted_keys(num_groups);
		for (const auto &[k, gid]: map) sorted_keys[gid] = &k;

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

	template<std::size_t N>
	void HashAggregate::ConsumeFastImpl() {
		HashMap<FastKeyN<N>, FastKeyNHash<N>> map;
		std::vector<std::uint32_t> gids;
		std::uint32_t num_groups = 0;

		while (auto eb = child_.Next()) {
			EvalContext ctx{eb->batch, eb->full_selection() ? nullptr : &eb->sel};

			std::vector<EvalCol> key_cols;
			key_cols.reserve(N);
			for (const auto &e: key_exprs_) key_cols.push_back(e->eval(ctx));

			const std::size_t n = ctx.rows();
			gids.assign(n, 0);
			for (std::size_t r = 0; r < n; ++r) {
				FastKeyN<N> k;
				for (std::size_t i = 0; i < N; ++i) {
					if (key_types_[i] == EvalType::Str) {
						const auto &v = std::get<std::vector<std::string>>(key_cols[i]);
						k.data[i] = static_cast<std::int64_t>(InternString(v[r]));
					} else {
						k.data[i] = std::visit([r](const auto &v) -> std::int64_t {
							using T = typename std::decay_t<decltype(v)>::value_type;
							if constexpr (std::is_arithmetic_v<T>) return static_cast<std::int64_t>(v[r]);
							else throw std::runtime_error("ConsumeFastImpl: unexpected non-numeric");
						}, key_cols[i]);
					}
				}

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

		std::vector<const FastKeyN<N> *> sorted_keys(num_groups);
		map.for_each([&](const FastKeyN<N> &k, std::uint32_t gid) { sorted_keys[gid] = &k; });

		for (std::uint32_t gid = 0; gid < num_groups; ++gid) {
			const FastKeyN<N> &fk = *sorted_keys[gid];
			for (std::size_t i = 0; i < N; ++i) {
				if (key_types_[i] == EvalType::Str) {
					std::get<DictColumn>(result_->GetColumn(i))
						.push_back(GetInternedString(static_cast<std::uint32_t>(fk.data[i])));
				} else {
					std::visit([v = fk.data[i]](auto &vec) {
						using T = typename std::decay_t<decltype(vec)>::value_type;
						if constexpr (std::is_integral_v<T>) vec.push_back(static_cast<T>(v));
						else throw std::runtime_error("ConsumeFastImpl emit: unexpected non-integer slot");
					}, result_->GetColumn(i));
				}
			}
		}
		for (std::size_t i = 0; i < aggs_.size(); ++i) {
			aggs_[i]->EmitInto(result_->GetColumn(N + i));
		}
		result_->SetRowCount(num_groups);
	}

	void HashAggregate::ConsumeFast() {
		switch (key_types_.size()) {
			case 1: ConsumeFastImpl<1>(); break;
			case 2: ConsumeFastImpl<2>(); break;
			case 3: ConsumeFastImpl<3>(); break;
			case 4: ConsumeFastImpl<4>(); break;
			default: throw std::runtime_error("ConsumeFast: unexpected key count");
		}
	}
}
