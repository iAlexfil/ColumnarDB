#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "expr.h"
#include "operator.h"
#include "schema.h"

namespace exec {
	namespace detail {
		inline void SortAppendBatch(Batch &dst, const Batch &src,
		                            const std::vector<std::uint32_t> *sel) {
			for (std::size_t c = 0; c < dst.ColCount(); ++c) {
				std::visit([&](auto &dv) {
					using V = std::decay_t<decltype(dv)>;
					const auto &sv = std::get<V>(src.GetColumn(c));
					if (sel == nullptr) {
						dv.insert(dv.end(), sv.begin(), sv.end());
					} else {
						dv.reserve(dv.size() + sel->size());
						for (std::uint32_t i: *sel) dv.push_back(sv[i]);
					}
				}, dst.GetColumn(c));
			}
			dst.SetRowCount(dst.RowCount() + (sel ? sel->size() : src.RowCount()));
		}

		inline int SortCompareAt(const EvalCol &c, std::uint32_t i, std::uint32_t j) {
			return std::visit([&](const auto &v) -> int {
				if (v[i] < v[j]) return -1;
				if (v[i] > v[j]) return 1;
				return 0;
			}, c);
		}

		inline void SortGatherInto(const DataVector &src,
		                           const std::vector<std::uint32_t> &idx,
		                           DataVector &dst) {
			std::visit([&](const auto &v) {
				using V = std::decay_t<decltype(v)>;
				using T = typename V::value_type;
				std::vector<T> out;
				out.reserve(idx.size());
				for (auto p: idx) out.push_back(v[p]);
				dst.template emplace<std::vector<T> >(std::move(out));
			}, src);
		}
	}

	class Sort final : public Operator {
	public:
		Sort(Operator &child,
		     std::vector<std::pair<ExprPtr, bool> > keys,
		     std::size_t limit = std::numeric_limits<std::size_t>::max(),
		     std::size_t offset = 0)
			: child_(child), limit_(limit), offset_(offset) {
			if (keys.empty()) throw std::runtime_error("Sort: at least one key required");
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
			const Schema &schema = child_.OutputSchema();

			auto accum = std::make_unique<Batch>(schema);
			while (auto eb = child_.Next()) {
				const std::vector<std::uint32_t> *sel = eb->full_selection() ? nullptr : &eb->sel;
				detail::SortAppendBatch(*accum, *eb->batch, sel);
			}

			const std::size_t n = accum->RowCount();

			EvalContext ctx{accum.get(), nullptr};
			std::vector<EvalCol> key_cols;
			for (const auto &e: key_exprs_) key_cols.push_back(e->eval(ctx));

			std::vector<std::uint32_t> idx(n);
			for (std::uint32_t i = 0; i < n; ++i) idx[i] = i;

			std::stable_sort(idx.begin(), idx.end(), [&](std::uint32_t a, std::uint32_t b) {
				for (std::size_t k = 0; k < key_cols.size(); ++k) {
					int c = detail::SortCompareAt(key_cols[k], a, b);
					if (c != 0) return key_asc_[k] ? (c < 0) : (c > 0);
				}
				return false;
			});

			const std::size_t start = std::min(offset_, idx.size());
			const std::size_t end = std::min(idx.size(), start + limit_);
			std::vector<std::uint32_t> trimmed(idx.begin() + start, idx.begin() + end);

			result_ = std::make_unique<Batch>(schema);
			for (std::size_t c = 0; c < schema.size(); ++c) {
				detail::SortGatherInto(accum->GetColumn(c), trimmed, result_->GetColumn(c));
			}
			result_->SetRowCount(trimmed.size());
		}

		Operator &child_;
		std::vector<ExprPtr> key_exprs_;
		std::vector<bool> key_asc_;
		std::size_t limit_;
		std::size_t offset_;
		bool consumed_ = false;
		std::unique_ptr<Batch> result_;
	};
}
