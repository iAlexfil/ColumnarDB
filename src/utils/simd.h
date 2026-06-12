#pragma once

#include <cstddef>
#include <cstdint>

#include <immintrin.h>

namespace simd {
	inline void CmpEqI64(const std::int64_t *l, const std::int64_t *r,
	                     std::uint8_t *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(l + i));
			__m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r + i));
			__m256i eq = _mm256_cmpeq_epi64(a, b);
			alignas(32) std::int64_t buf[4];
			_mm256_store_si256(reinterpret_cast<__m256i *>(buf), eq);
			out[i + 0] = buf[0] ? 1 : 0;
			out[i + 1] = buf[1] ? 1 : 0;
			out[i + 2] = buf[2] ? 1 : 0;
			out[i + 3] = buf[3] ? 1 : 0;
		}
		for (; i < n; ++i) out[i] = (l[i] == r[i]) ? 1 : 0;
	}

	inline void CmpGtI64(const std::int64_t *l, const std::int64_t *r,
	                     std::uint8_t *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(l + i));
			__m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r + i));
			__m256i gt = _mm256_cmpgt_epi64(a, b);
			alignas(32) std::int64_t buf[4];
			_mm256_store_si256(reinterpret_cast<__m256i *>(buf), gt);
			out[i + 0] = buf[0] ? 1 : 0;
			out[i + 1] = buf[1] ? 1 : 0;
			out[i + 2] = buf[2] ? 1 : 0;
			out[i + 3] = buf[3] ? 1 : 0;
		}
		for (; i < n; ++i) out[i] = (l[i] > r[i]) ? 1 : 0;
	}

	inline void AddI64(const std::int64_t *l, const std::int64_t *r,
	                   std::int64_t *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(l + i));
			__m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r + i));
			_mm256_storeu_si256(reinterpret_cast<__m256i *>(out + i),
			                    _mm256_add_epi64(a, b));
		}
		for (; i < n; ++i) out[i] = l[i] + r[i];
	}

	inline void SubI64(const std::int64_t *l, const std::int64_t *r,
	                   std::int64_t *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(l + i));
			__m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(r + i));
			_mm256_storeu_si256(reinterpret_cast<__m256i *>(out + i),
			                    _mm256_sub_epi64(a, b));
		}
		for (; i < n; ++i) out[i] = l[i] - r[i];
	}

	inline void AddF64(const double *l, const double *r, double *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256d a = _mm256_loadu_pd(l + i);
			__m256d b = _mm256_loadu_pd(r + i);
			_mm256_storeu_pd(out + i, _mm256_add_pd(a, b));
		}
		for (; i < n; ++i) out[i] = l[i] + r[i];
	}

	inline void SubF64(const double *l, const double *r, double *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256d a = _mm256_loadu_pd(l + i);
			__m256d b = _mm256_loadu_pd(r + i);
			_mm256_storeu_pd(out + i, _mm256_sub_pd(a, b));
		}
		for (; i < n; ++i) out[i] = l[i] - r[i];
	}

	inline void MulF64(const double *l, const double *r, double *out, std::size_t n) {
		std::size_t i = 0;
		for (; i + 4 <= n; i += 4) {
			__m256d a = _mm256_loadu_pd(l + i);
			__m256d b = _mm256_loadu_pd(r + i);
			_mm256_storeu_pd(out + i, _mm256_mul_pd(a, b));
		}
		for (; i < n; ++i) out[i] = l[i] * r[i];
	}
}
