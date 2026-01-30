#include "columnar_writer.h"

#include <limits>
#include <stdexcept>

#include "batch.h"
#include "utils/utils.h"

template<class T>
void WriteObj(std::ofstream &out, const T &v) {
	out.write(reinterpret_cast<const char *>(&v), sizeof(T));
	if (!out) {
		throw std::runtime_error("failed to write to file");
	}
}

void WriteBytes(std::ofstream &out, const void *data, std::size_t size) {
	out.write(reinterpret_cast<const char *>(data), static_cast<std::streamsize>(size));
	if (!out) {
		throw std::runtime_error("failed to write to file");
	}
}

std::uint64_t Position(std::ofstream &out) {
	return out.tellp();
}

void WriteString(std::ofstream &out, const std::string &s) {
	const auto len = static_cast<std::uint32_t>(s.size());
	WriteObj(out, len);
	WriteBytes(out, s.data(), s.size());
}


namespace columnar {
	ColumnarWriter::ColumnarWriter(const std::filesystem::path &path, const Schema &schema)
		: out_(path, std::ios::binary | std::ios::trunc)
		  , schema_(schema) {
		if (!out_.is_open()) {
			throw std::runtime_error("failed to open file for writing: " + path.string());
		}
		if (schema_.empty()) {
			throw std::runtime_error("columnar: invalid schema");
		}
		WriteHeader();
	}

	ColumnarWriter::~ColumnarWriter() {
		if (!finalized_) {
			Finish();
		}
	}

	void ColumnarWriter::WriteHeader() {
		const char magic[4] = {'C', 'D', 'B', '1'};
		WriteBytes(out_, magic, sizeof(magic));
		WriteObj(out_, kColumnarVersion);
		WriteObj(out_, static_cast<std::uint64_t>(0));
	}

	void ColumnarWriter::WriteBatch(const Batch &batch) {
		if (finalized_) {
			throw std::runtime_error("columnar: cannot write row group after Finalize()");
		}

		const std::size_t ncols = schema_.size();
		const std::size_t nrows = batch.RowCount();

		BatchMeta rg;
		rg.row_count = nrows;
		rg.columns.resize(ncols);

		WriteObj(out_, rg.row_count);

		for (std::size_t col = 0; col < ncols; ++col) {
			const std::uint64_t chunk_begin = Position(out_);

			const auto &col_schema = schema_[col];
			const auto &column = batch.GetColumn(col);

			switch (col_schema.type) {
				case DataType::Int64: {
					const auto &vec = std::get<std::vector<std::int64_t> >(column);
					if (!vec.empty()) {
						WriteBytes(out_, vec.data(), vec.size() * sizeof(std::int64_t));
					}
					break;
				}
				case DataType::String: {
					const auto &vec = std::get<std::vector<std::string> >(column);

					for (const auto &s: vec) {
						WriteObj(out_, static_cast<std::uint32_t>(s.size()));
					}

					for (const auto &s: vec) {
						if (!s.empty()) {
							WriteBytes(out_, s.data(), s.size());
						}
					}
					break;
				}
				default:
					throw std::runtime_error("columnar: unsupported DataType in schema");
			}

			const std::uint64_t chunk_end = Position(out_);
			rg.columns[col].offset = chunk_begin;
			rg.columns[col].size = chunk_end - chunk_begin;
		}

		batches_.push_back(std::move(rg));
	}

	void ColumnarWriter::PatchFooterOffset(std::uint64_t footer_offset) {
		// header: magic(4) + version(4) + footer_offset(8)
		constexpr std::uint64_t footer_pos_in_header = 4 + 4;
		const std::uint64_t cur = Position(out_);
		utils::Seek(out_, footer_pos_in_header);
		WriteObj(out_, footer_offset);
		utils::Seek(out_, cur);
	}

	void ColumnarWriter::WriteFooter(std::uint64_t footer_offset) {
		WriteObj(out_, static_cast<std::uint32_t>(schema_.size()));
		for (const auto &col: schema_) {
			WriteString(out_, col.name);
			const auto type = static_cast<std::uint8_t>(col.type);
			WriteObj(out_, type);
		}

		const auto nrg = static_cast<std::uint32_t>(batches_.size());
		WriteObj(out_, nrg);
		for (const auto &rg: batches_) {
			WriteObj(out_, rg.row_count);
			for (const auto &ch: rg.columns) {
				WriteObj(out_, ch.offset);
				WriteObj(out_, ch.size);
			}
		}
	}

	void ColumnarWriter::Finish() {
		if (finalized_) return;
		finalized_ = true;

		out_.flush();

		const std::uint64_t footer_offset = Position(out_);
		WriteFooter(footer_offset);

		out_.flush();

		PatchFooterOffset(footer_offset);

		out_.flush();
	}
}
