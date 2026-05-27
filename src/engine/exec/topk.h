#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "expr.h"
#include "operator.h"
#include "schema.h"

namespace exec {
	namespace detail {
		using TopKKeyVal = std::variant<std::int64_t, std::uint64_t, double, std::string, std::uint8_t>;

		inline int CompareKeyVal(const TopKKeyVal &a, const TopKKeyVal &b) {
			return std::visit([](const auto &x, const auto &y) -> int {
				using X = std::decay_t<decltype(x)>;
				using Y = std::decay_t<decltype(y)>;
				if constexpr (std::is_same_v<X, Y>) {
					if (x < y) return -1;
					if (x > y) return 1;
					return 0;
				} else {
					return 0;
				}
			}, a, b);
		}

		struct TopKCandidate {
			std::vector<TopKKeyVal> keys;
			std::vector<DataObject> row;
		};

		inline std::vector<TopKKeyVal> ExtractKeys(const std::vector<EvalCol> &key_cols, std::size_t row) {
			std::vector<TopKKeyVal> out;
			out.reserve(key_cols.size());
			for (const auto &kc: key_cols) {
				std::visit([&](const auto &v) { out.emplace_back(v[row]); }, kc);
			}
			return out;
		}

		inline std::vector<DataObject> ExtractRow(const Batch &batch, std::uint32_t src_row) {
			std::vector<DataObject> out;
			out.reserve(batch.ColCount());
			for (std::size_t c = 0; c < batch.ColCount(); ++c) {
				std::visit([&](const auto &v) { out.emplace_back(v[src_row]); }, batch.GetColumn(c));
			}
			return out;
		}
	}

	class TopK final : public Operator {
	public:
		TopK(Operator &child, std::vector<std::pair<ExprPtr, bool> > keys, std::size_t k)
			: child_(child), k_(k) {
			if (keys.empty()) throw std::runtime_error("TopK: at least one key required");
			for (auto &[e, asc]: keys) {
				key_exprs_.push_back(std::move(e));
				key_asc_.push_back(asc);
			}
		}

		const Schema &OutputSchema() const override { return child_.OutputSchema(); }

		std::optional<ExecBatch> Next() override {
			if (!consumed_) {
				Consume();
				consumed_ = true;
				ExecBatch out;
				out.batch = result_.get();
				return out;
			}
			return std::nullopt;
		}

	private:
		void Consume() {
			using detail::TopKCandidate;

			auto better = [&](const TopKCandidate &a, const TopKCandidate &b) -> bool {
				for (std::size_t i = 0; i < key_asc_.size(); ++i) {
					int c = detail::CompareKeyVal(a.keys[i], b.keys[i]);
					if (c == 0) continue;
					return key_asc_[i] ? (c < 0) : (c > 0);
				}
				return false;
			};

			std::vector<TopKCandidate> heap;
			heap.reserve(k_);

			while (auto eb = child_.Next()) {
				EvalContext ctx{eb->batch, eb->full_selection() ? nullptr : &eb->sel};

				std::vector<EvalCol> key_cols;
				for (const auto &e: key_exprs_) key_cols.push_back(e->eval(ctx));

				const std::size_t nrows = ctx.rows();
				for (std::size_t r = 0; r < nrows; ++r) {
					TopKCandidate cand;
					cand.keys = detail::ExtractKeys(key_cols, r);

					bool should_take = heap.size() < k_ || better(cand, heap.front());
					if (!should_take) continue;

					std::uint32_t src_row = eb->full_selection()
						                        ? static_cast<std::uint32_t>(r)
						                        : eb->sel[r];
					cand.row = detail::ExtractRow(*eb->batch, src_row);

					if (heap.size() < k_) {
						heap.push_back(std::move(cand));
						std::push_heap(heap.begin(), heap.end(), better);
					} else {
						std::pop_heap(heap.begin(), heap.end(), better);
						heap.back() = std::move(cand);
						std::push_heap(heap.begin(), heap.end(), better);
					}
				}
			}

			std::sort(heap.begin(), heap.end(), better);

			const Schema &sch = child_.OutputSchema();
			result_ = std::make_unique<Batch>(sch);
			result_->Reserve(heap.size());
			for (const auto &c: heap) {
				for (std::size_t i = 0; i < sch.size(); ++i) {
					std::visit([&](const auto &x) {
						using T = std::decay_t<decltype(x)>;
						std::get<std::vector<T> >(result_->GetColumn(i)).push_back(x);
					}, c.row[i]);
				}
			}
			result_->SetRowCount(heap.size());
		}

		Operator &child_;
		std::vector<ExprPtr> key_exprs_;
		std::vector<bool> key_asc_;
		std::size_t k_;
		bool consumed_ = false;
		std::unique_ptr<Batch> result_;
	};
}
