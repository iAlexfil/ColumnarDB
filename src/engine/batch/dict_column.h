#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "utils/hash_map.h"

class DictColumn {
public:
	using value_type = std::string;

	DictColumn() = default;

	DictColumn(const DictColumn &o) : dict_(o.dict_), codes_(o.codes_) {
		rebuild_index();
	}

	DictColumn(DictColumn &&o) noexcept
		: dict_(std::move(o.dict_)), codes_(std::move(o.codes_)) {
		rebuild_index();
		o.index_.clear();
	}

	DictColumn &operator=(const DictColumn &o) {
		if (this != &o) {
			dict_ = o.dict_;
			codes_ = o.codes_;
			rebuild_index();
		}
		return *this;
	}

	DictColumn &operator=(DictColumn &&o) noexcept {
		if (this != &o) {
			dict_ = std::move(o.dict_);
			codes_ = std::move(o.codes_);
			rebuild_index();
			o.index_.clear();
		}
		return *this;
	}

	std::size_t size() const { return codes_.size(); }
	bool empty() const { return codes_.empty(); }

	const std::string &operator[](std::size_t i) const { return dict_[codes_[i]]; }

	const std::deque<std::string> &dictionary() const { return dict_; }
	const std::vector<std::uint32_t> &codes() const { return codes_; }

	void push_back(const std::string &s) {
		auto *p = index_.find(s);
		if (p) {
			codes_.push_back(*p);
		} else {
			auto id = static_cast<std::uint32_t>(dict_.size());
			dict_.push_back(s);
			index_.insert(std::string_view(dict_.back()), id);
			codes_.push_back(id);
		}
	}

	void push_back(std::string &&s) {
		auto *p = index_.find(s);
		if (p) {
			codes_.push_back(*p);
		} else {
			auto id = static_cast<std::uint32_t>(dict_.size());
			dict_.push_back(std::move(s));
			index_.insert(std::string_view(dict_.back()), id);
			codes_.push_back(id);
		}
	}

	void reserve(std::size_t n) { codes_.reserve(n); }

	void resize(std::size_t n) {
		if (dict_.empty()) {
			dict_.emplace_back("");
			index_.insert(std::string_view(dict_.back()), 0);
		}
		codes_.resize(n, 0);
	}

	void clear() {
		codes_.clear();
		dict_.clear();
		index_.clear();
	}

	void load_dict(std::vector<std::string> dict_vec, const std::uint8_t *codes_raw, std::size_t nrows) {
		dict_.assign(std::make_move_iterator(dict_vec.begin()), std::make_move_iterator(dict_vec.end()));
		index_.clear();
		for (std::uint32_t i = 0; i < dict_.size(); ++i) {
			index_.insert(std::string_view(dict_[i]), i);
		}
		codes_.resize(nrows);
		if (nrows > 0) {
			std::memcpy(codes_.data(), codes_raw, nrows * sizeof(std::uint32_t));
		}
	}

	bool operator==(const DictColumn &o) const {
		if (size() != o.size()) return false;
		for (std::size_t i = 0; i < size(); ++i) {
			if ((*this)[i] != o[i]) return false;
		}
		return true;
	}

	bool operator==(const std::vector<std::string> &o) const {
		if (size() != o.size()) return false;
		for (std::size_t i = 0; i < size(); ++i) {
			if ((*this)[i] != o[i]) return false;
		}
		return true;
	}

	friend bool operator==(const std::vector<std::string> &a, const DictColumn &b) {
		return b == a;
	}

private:
	void rebuild_index() {
		index_.clear();
		for (std::uint32_t i = 0; i < dict_.size(); ++i) {
			index_.insert(std::string_view(dict_[i]), i);
		}
	}

	std::deque<std::string> dict_;
	std::vector<std::uint32_t> codes_;
	HashMap<std::string_view, std::hash<std::string_view>> index_;
};
