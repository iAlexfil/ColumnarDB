#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

class DictColumn {
public:
	using value_type = std::string;

	DictColumn() = default;

	std::size_t size() const { return codes_.size(); }
	bool empty() const { return codes_.empty(); }

	const std::string &operator[](std::size_t i) const { return dict_[codes_[i]]; }
	std::uint32_t code_at(std::size_t i) const { return codes_[i]; }

	const std::deque<std::string> &dictionary() const { return dict_; }
	const std::vector<std::uint32_t> &codes() const { return codes_; }
	std::size_t dict_size() const { return dict_.size(); }

	void push_back(const std::string &s) {
		auto it = index_.find(s);
		if (it != index_.end()) {
			codes_.push_back(it->second);
		} else {
			auto id = static_cast<std::uint32_t>(dict_.size());
			dict_.push_back(s);
			index_.emplace(std::string_view(dict_.back()), id);
			codes_.push_back(id);
		}
	}

	void push_back(std::string &&s) {
		auto it = index_.find(s);
		if (it != index_.end()) {
			codes_.push_back(it->second);
		} else {
			auto id = static_cast<std::uint32_t>(dict_.size());
			dict_.push_back(std::move(s));
			index_.emplace(std::string_view(dict_.back()), id);
			codes_.push_back(id);
		}
	}

	void reserve(std::size_t n) { codes_.reserve(n); }

	void resize(std::size_t n) {
		if (dict_.empty()) {
			dict_.emplace_back("");
			index_.emplace(std::string_view(dict_.back()), 0);
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
			index_.emplace(std::string_view(dict_[i]), i);
		}
		codes_.resize(nrows);
		if (nrows > 0) {
			std::memcpy(codes_.data(), codes_raw, nrows * sizeof(std::uint32_t));
		}
	}

	std::vector<std::string> materialize() const {
		std::vector<std::string> out(codes_.size());
		for (std::size_t i = 0; i < codes_.size(); ++i) out[i] = dict_[codes_[i]];
		return out;
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
	std::deque<std::string> dict_;
	std::vector<std::uint32_t> codes_;
	std::unordered_map<std::string_view, std::uint32_t> index_;
};
