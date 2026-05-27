#pragma once

#include <climits>
#include <cstdint>
#include <vector>

template<class Key, class Hash>
class HashMap {
	static constexpr std::uint32_t kEmpty = UINT32_MAX;
public:
	HashMap() : mask_(15), size_(0) {
		slots_.resize(16);
		vals_.resize(16, kEmpty);
	}

	std::uint32_t* find(const Key &k) {
		std::size_t h = hash_(k);
		for (std::size_t step = 0; ; ++step) {
			std::size_t idx = (h + step * (step + 1) / 2) & mask_;
			if (vals_[idx] == kEmpty) return nullptr;
			if (slots_[idx] == k) return &vals_[idx];
		}
	}

	void insert(const Key &k, std::uint32_t v) {
		if (size_ * 2 >= mask_ + 1) grow();
		std::size_t h = hash_(k);
		for (std::size_t step = 0; ; ++step) {
			std::size_t idx = (h + step * (step + 1) / 2) & mask_;
			if (vals_[idx] == kEmpty) {
				slots_[idx] = k;
				vals_[idx] = v;
				++size_;
				return;
			}
		}
	}

	template<class Fn>
	void for_each(Fn fn) const {
		for (std::size_t i = 0; i <= mask_; ++i) {
			if (vals_[i] != kEmpty) fn(slots_[i], vals_[i]);
		}
	}

private:
	void grow() {
		std::size_t new_cap = (mask_ + 1) * 2;
		std::vector<Key> old_slots = std::move(slots_);
		std::vector<std::uint32_t> old_vals = std::move(vals_);
		mask_ = new_cap - 1;
		size_ = 0;
		slots_.resize(new_cap);
		vals_.assign(new_cap, kEmpty);
		for (std::size_t i = 0; i < old_vals.size(); ++i) {
			if (old_vals[i] != kEmpty) insert(old_slots[i], old_vals[i]);
		}
	}

	Hash hash_;
	std::vector<Key> slots_;
	std::vector<std::uint32_t> vals_;
	std::size_t mask_;
	std::size_t size_;
};
