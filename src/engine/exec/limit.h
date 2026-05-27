#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "operator.h"

namespace exec {

class Limit final : public Operator {
public:
	Limit(Operator &child, std::size_t n) : child_(child), remaining_(n) {}

	const Schema &OutputSchema() const override { return child_.OutputSchema(); }

	std::optional<ExecBatch> Next() override {
		if (remaining_ == 0) return std::nullopt;
		auto eb = child_.Next();
		if (!eb) return std::nullopt;

		std::size_t avail = eb->size();
		if (avail <= remaining_) {
			remaining_ -= avail;
			return eb;
		}

		ExecBatch out;
		out.batch = eb->batch;
		if (eb->full_selection()) {
			sel_buf_.resize(remaining_);
			for (std::size_t i = 0; i < remaining_; ++i) sel_buf_[i] = static_cast<std::uint32_t>(i);
		} else {
			sel_buf_.assign(eb->sel.begin(), eb->sel.begin() + remaining_);
		}
		out.sel = sel_buf_;
		remaining_ = 0;
		return out;
	}

private:
	Operator &child_;
	std::size_t remaining_;
	std::vector<std::uint32_t> sel_buf_;
};

}
