#include "columnar_reader.h"

#include <stdexcept>
#include <string>

#include "batch.h"
#include "columnar_format.h"
#include "utils/utils.h"


template<class T>
T ReadObj(std::ifstream &in) {
	T v{};
	in.read(reinterpret_cast<char *>(&v), sizeof(T));
	if (!in) {
		throw std::runtime_error("failed to read from file");
	}
	return v;
}

void ReadBytes(std::ifstream &in, void *data, std::size_t size) {
	in.read(static_cast<char *>(data), static_cast<std::streamsize>(size));
	if (!in) throw std::runtime_error("failed to read from file");
}

std::string ReadString(std::ifstream &in) {
	const auto len = ReadObj<std::uint32_t>(in);
	std::string s;
	s.resize(len);
	if (len > 0) ReadBytes(in, s.data(), len);
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


namespace columnar {
	ColumnarReader::ColumnarReader(const std::filesystem::path &path)
		: in_(path, std::ios::binary) {
		if (!in_.is_open()) {
			throw std::runtime_error("columnar: failed to open file for reading: " + path.string());
		}
		ReadHeader();
		ReadFooter();
	}

	void ColumnarReader::ReadHeader() {
		char magic[4];
		ReadBytes(in_, magic, sizeof(magic));
		if (!(magic[0] == 'C' && magic[1] == 'D' && magic[2] == 'B' && magic[3] == '1')) {
			throw std::runtime_error("columnar: bad format file");
		}

		const auto version = ReadObj<std::uint32_t>(in_);
		if (version != kColumnarVersion) {
			throw std::runtime_error("unsupported version: " + std::to_string(version));
		}

		footer_offset_ = ReadObj<std::uint64_t>(in_);
		if (footer_offset_ == 0) {
			throw std::runtime_error("bad footer offset");
		}
	}

	void ColumnarReader::ReadFooter() {
		utils::Seek(in_, footer_offset_);

		const auto ncols = ReadObj<std::uint32_t>(in_);

		schema_.clear();
		schema_.reserve(ncols);
		for (std::uint32_t i = 0; i < ncols; ++i) {
			std::string name = ReadString(in_);
			schema_.push_back(ColumnSchema{std::move(name), ToDataType(ReadObj<std::uint8_t>(in_))});
		}

		const auto nrg = ReadObj<std::uint32_t>(in_);
		batches_.clear();
		batches_.reserve(nrg);
		for (std::uint32_t rg = 0; rg < nrg; ++rg) {
			BatchMeta meta;
			meta.row_count = ReadObj<std::uint32_t>(in_);
			meta.columns.resize(ncols);
			for (std::uint32_t c = 0; c < ncols; ++c) {
				meta.columns[c].offset = ReadObj<std::uint64_t>(in_);
				meta.columns[c].size = ReadObj<std::uint64_t>(in_);
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
		utils::Seek(in_, ch.offset);

		auto readFixed = [&]<class T>(std::vector<T> &vec) {
			vec.resize(nrows);
			if (nrows > 0) ReadBytes(in_, vec.data(), nrows * sizeof(T));
		};

		switch (cs.type) {
			case DataType::Int8:     readFixed(std::get<std::vector<std::int8_t>>(dst)); return;
			case DataType::Int16:    readFixed(std::get<std::vector<std::int16_t>>(dst)); return;
			case DataType::Int32:    readFixed(std::get<std::vector<std::int32_t>>(dst)); return;
			case DataType::Int64:    readFixed(std::get<std::vector<std::int64_t>>(dst)); return;
			case DataType::UInt8:    readFixed(std::get<std::vector<std::uint8_t>>(dst)); return;
			case DataType::UInt16:   readFixed(std::get<std::vector<std::uint16_t>>(dst)); return;
			case DataType::UInt32:   readFixed(std::get<std::vector<std::uint32_t>>(dst)); return;
			case DataType::UInt64:   readFixed(std::get<std::vector<std::uint64_t>>(dst)); return;
			case DataType::Float32:  readFixed(std::get<std::vector<float>>(dst)); return;
			case DataType::Float64:  readFixed(std::get<std::vector<double>>(dst)); return;
			case DataType::Date:     readFixed(std::get<std::vector<std::int32_t>>(dst)); return;
			case DataType::DateTime: readFixed(std::get<std::vector<std::int64_t>>(dst)); return;
			case DataType::String: {
				auto &vec = std::get<std::vector<std::string>>(dst);
				vec.resize(nrows);
				std::vector<std::uint32_t> lens(nrows);
				if (nrows > 0) ReadBytes(in_, lens.data(), nrows * sizeof(std::uint32_t));
				for (std::size_t i = 0; i < nrows; ++i) {
					vec[i].resize(lens[i]);
					if (lens[i] > 0) ReadBytes(in_, vec[i].data(), lens[i]);
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
		for (auto c : col_indices) {
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
