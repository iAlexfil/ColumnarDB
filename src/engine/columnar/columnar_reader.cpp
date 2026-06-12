#include "columnar_reader.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "batch.h"
#include "columnar_format.h"

namespace {
	template<class T>
	T ReadAt(const std::uint8_t *base, std::size_t off) {
		T v;
		std::memcpy(&v, base + off, sizeof(T));
		return v;
	}

	std::string ReadStringAt(const std::uint8_t *base, std::size_t &off) {
		const auto len = ReadAt<std::uint32_t>(base, off);
		off += sizeof(std::uint32_t);
		std::string s(reinterpret_cast<const char *>(base + off), len);
		off += len;
		return s;
	}

	DataType ToDataType(std::uint8_t raw) {
		switch (static_cast<DataType>(raw)) {
			case DataType::Int8:
			case DataType::Int16:
			case DataType::Int32:
			case DataType::Int64:
			case DataType::UInt8:
			case DataType::UInt16:
			case DataType::UInt32:
			case DataType::UInt64:
			case DataType::Float32:
			case DataType::Float64:
			case DataType::String:
			case DataType::Date:
			case DataType::DateTime:
				return static_cast<DataType>(raw);
		}
		throw std::runtime_error("unknown DataType");
	}
}

namespace columnar {
	ColumnarReader::ColumnarReader(const std::filesystem::path &path) {
		fd_ = ::open(path.c_str(), O_RDONLY);
		if (fd_ < 0) {
			throw std::runtime_error("columnar: failed to open file for reading: " + path.string());
		}

		struct stat st{};
		if (::fstat(fd_, &st) != 0) {
			::close(fd_);
			fd_ = -1;
			throw std::runtime_error("columnar: fstat failed");
		}
		mapped_size_ = static_cast<std::size_t>(st.st_size);

		void *m = ::mmap(nullptr, mapped_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
		if (m == MAP_FAILED) {
			::close(fd_);
			fd_ = -1;
			throw std::runtime_error("columnar: mmap failed");
		}
		mapped_ = static_cast<const std::uint8_t *>(m);

		ReadHeader();
		ReadFooter();
	}

	void ColumnarReader::Prefetch(std::size_t batch_idx,
	                              const std::vector<std::size_t> &col_indices) const {
		if (batch_idx >= batches_.size() || fd_ < 0) return;
		const auto &meta = batches_[batch_idx];
		for (auto c: col_indices) {
			if (c >= meta.columns.size()) continue;
			const auto &ch = meta.columns[c];
			if (ch.size == 0) continue;
			readahead(fd_, static_cast<off64_t>(ch.offset), ch.size);
		}
	}

	ColumnarReader::~ColumnarReader() {
		if (mapped_) {
			munmap(const_cast<std::uint8_t *>(mapped_), mapped_size_);
			mapped_ = nullptr;
		}
		if (fd_ >= 0) {
			close(fd_);
			fd_ = -1;
		}
	}

	void ColumnarReader::ReadHeader() {
		if (mapped_size_ < 16) throw std::runtime_error("columnar: file too small");
		const char *magic = reinterpret_cast<const char *>(mapped_);
		if (!(magic[0] == 'C' && magic[1] == 'D' && magic[2] == 'B' && magic[3] == '1')) {
			throw std::runtime_error("columnar: bad format file");
		}
		const auto version = ReadAt<std::uint32_t>(mapped_, 4);
		if (version != kColumnarVersion) {
			throw std::runtime_error("unsupported version: " + std::to_string(version));
		}
		footer_offset_ = ReadAt<std::uint64_t>(mapped_, 8);
		if (footer_offset_ == 0 || footer_offset_ >= mapped_size_) {
			throw std::runtime_error("bad footer offset");
		}
	}

	void ColumnarReader::ReadFooter() {
		std::size_t off = footer_offset_;
		const auto ncols = ReadAt<std::uint32_t>(mapped_, off);
		off += sizeof(std::uint32_t);

		schema_.clear();
		schema_.reserve(ncols);
		for (std::uint32_t i = 0; i < ncols; ++i) {
			std::string name = ReadStringAt(mapped_, off);
			std::uint8_t raw = ReadAt<std::uint8_t>(mapped_, off);
			off += sizeof(std::uint8_t);
			schema_.push_back(ColumnSchema{std::move(name), ToDataType(raw)});
		}

		const auto nrg = ReadAt<std::uint32_t>(mapped_, off);
		off += sizeof(std::uint32_t);

		batches_.clear();
		batches_.reserve(nrg);
		for (std::uint32_t rg = 0; rg < nrg; ++rg) {
			BatchMeta meta;
			meta.row_count = ReadAt<std::uint32_t>(mapped_, off);
			off += sizeof(std::uint32_t);
			meta.columns.resize(ncols);
			for (std::uint32_t c = 0; c < ncols; ++c) {
				meta.columns[c].offset = ReadAt<std::uint64_t>(mapped_, off);
				off += sizeof(std::uint64_t);
				meta.columns[c].size = ReadAt<std::uint64_t>(mapped_, off);
				off += sizeof(std::uint64_t);
				meta.columns[c].encoding = static_cast<Encoding>(ReadAt<std::uint8_t>(mapped_, off));
				off += sizeof(std::uint8_t);
			}
			batches_.push_back(std::move(meta));
		}

		for (const auto &rg: batches_) {
			for (const auto &ch: rg.columns) {
				if (ch.size > footer_offset_ || ch.offset > footer_offset_ - ch.size) {
					throw std::runtime_error("invalid meta data in .columnar file");
				}
			}
		}
	}

	void ColumnarReader::ReadOneColumn(std::size_t batch_idx, std::size_t src_col,
	                                   DataVector &dst, std::size_t nrows) {
		const auto &cs = schema_[src_col];
		const auto &ch = batches_[batch_idx].columns[src_col];
		const std::uint8_t *p = mapped_ + ch.offset;

		auto readFixed = [&]<class T>(std::vector<T> &vec) {
			vec.resize(nrows);
			if (nrows == 0) return;
			if (ch.encoding == Encoding::RLE) {
				std::size_t off = 0;
				const auto num_runs = ReadAt<std::uint32_t>(p, off);
				off += sizeof(std::uint32_t);
				std::size_t pos = 0;
				for (std::uint32_t r = 0; r < num_runs; ++r) {
					const auto val = ReadAt<T>(p, off);
					off += sizeof(T);
					const auto cnt = ReadAt<std::uint32_t>(p, off);
					off += sizeof(std::uint32_t);
					for (std::uint32_t k = 0; k < cnt; ++k) vec[pos++] = val;
				}
			} else {
				std::memcpy(vec.data(), p, nrows * sizeof(T));
			}
		};

		switch (cs.type) {
			case DataType::Int8: readFixed(std::get<std::vector<std::int8_t> >(dst));
				return;
			case DataType::Int16: readFixed(std::get<std::vector<std::int16_t> >(dst));
				return;
			case DataType::Int32: readFixed(std::get<std::vector<std::int32_t> >(dst));
				return;
			case DataType::Int64: readFixed(std::get<std::vector<std::int64_t> >(dst));
				return;
			case DataType::UInt8: readFixed(std::get<std::vector<std::uint8_t> >(dst));
				return;
			case DataType::UInt16: readFixed(std::get<std::vector<std::uint16_t> >(dst));
				return;
			case DataType::UInt32: readFixed(std::get<std::vector<std::uint32_t> >(dst));
				return;
			case DataType::UInt64: readFixed(std::get<std::vector<std::uint64_t> >(dst));
				return;
			case DataType::Float32: readFixed(std::get<std::vector<float> >(dst));
				return;
			case DataType::Float64: readFixed(std::get<std::vector<double> >(dst));
				return;
			case DataType::Date: readFixed(std::get<std::vector<std::int32_t> >(dst));
				return;
			case DataType::DateTime: readFixed(std::get<std::vector<std::int64_t> >(dst));
				return;
			case DataType::String: {
				auto &dc = std::get<DictColumn>(dst);
				dc.clear();
				if (ch.encoding == Encoding::Dict) {
					std::size_t off = 0;
					const auto dict_size = ReadAt<std::uint32_t>(p, off);
					off += sizeof(std::uint32_t);
					std::vector<std::string> dict(dict_size);
					for (std::uint32_t d = 0; d < dict_size; ++d) {
						const auto slen = ReadAt<std::uint32_t>(p, off);
						off += sizeof(std::uint32_t);
						dict[d].assign(reinterpret_cast<const char*>(p + off), slen);
						off += slen;
					}
					dc.load_dict(std::move(dict), p + off, nrows);
				} else {
					dc.reserve(nrows);
					const std::uint32_t *lens = reinterpret_cast<const std::uint32_t *>(p);
					const std::uint8_t *data = p + nrows * sizeof(std::uint32_t);
					std::size_t off = 0;
					for (std::size_t i = 0; i < nrows; ++i) {
						dc.push_back(std::string(reinterpret_cast<const char *>(data + off), lens[i]));
						off += lens[i];
					}
				}
				return;
			}
		}
		throw std::runtime_error("columnar: unsupported DataType");
	}

	Batch ColumnarReader::ReadBatchColumns(std::size_t batch_idx,
	                                       const std::vector<std::size_t> &col_indices) {
		if (batch_idx >= batches_.size()) {
			throw std::runtime_error("columnar: batch index out of range");
		}
		Schema sub;
		sub.reserve(col_indices.size());
		for (auto c: col_indices) {
			if (c >= schema_.size()) throw std::runtime_error("columnar: projection column index out of range");
			sub.push_back(schema_[c]);
		}

		const std::size_t nrows = batches_[batch_idx].row_count;
		Batch batch(sub);
		batch.Reserve(nrows);

		for (std::size_t out_idx = 0; out_idx < col_indices.size(); ++out_idx) {
			ReadOneColumn(batch_idx, col_indices[out_idx], batch.GetColumn(out_idx), nrows);
		}
		batch.SetRowCount(nrows);
		return batch;
	}

	Batch ColumnarReader::ReadBatch(std::size_t idx) {
		std::vector<std::size_t> all(schema_.size());
		for (std::size_t i = 0; i < schema_.size(); ++i) all[i] = i;
		return ReadBatchColumns(idx, all);
	}
}
