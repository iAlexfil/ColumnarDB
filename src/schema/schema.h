#pragma once

#include <cctype>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "csvreader.h"
#include "csvwriter.h"
#include "utils/utils.h"

struct ColumnSchema {
	std::string name;
	DataType type;
};

using Schema = std::vector<ColumnSchema>;

inline std::string ToLowerAscii(std::string_view s) {
	std::string out;
	out.reserve(s.size());
	for (unsigned char ch : s) {
		if (ch >= 'A' && ch <= 'Z') ch = static_cast<unsigned char>(ch + 32);
		out.push_back(static_cast<char>(ch));
	}
	return out;
}

inline DataType ParseColumnType(std::string_view s) {
	s = utils::Trim(s);
	const std::string low = ToLowerAscii(s);

	if (low == "int8")     return DataType::Int8;
	if (low == "int16")    return DataType::Int16;
	if (low == "int32")    return DataType::Int32;
	if (low == "int64")    return DataType::Int64;
	if (low == "uint8")    return DataType::UInt8;
	if (low == "uint16")   return DataType::UInt16;
	if (low == "uint32")   return DataType::UInt32;
	if (low == "uint64")   return DataType::UInt64;
	if (low == "float32" || low == "float")  return DataType::Float32;
	if (low == "float64" || low == "double") return DataType::Float64;
	if (low == "string")   return DataType::String;
	if (low == "date")     return DataType::Date;
	if (low == "datetime" || low == "timestamp") return DataType::DateTime;
	if (low == "char")     return DataType::String;

	throw std::runtime_error("unknown column type: '" + std::string(s) + "'");
}

inline std::string ToString(DataType t) {
	switch (t) {
		case DataType::Int8:     return "int8";
		case DataType::Int16:    return "int16";
		case DataType::Int32:    return "int32";
		case DataType::Int64:    return "int64";
		case DataType::UInt8:    return "uint8";
		case DataType::UInt16:   return "uint16";
		case DataType::UInt32:   return "uint32";
		case DataType::UInt64:   return "uint64";
		case DataType::Float32:  return "float32";
		case DataType::Float64:  return "float64";
		case DataType::String:   return "string";
		case DataType::Date:     return "date";
		case DataType::DateTime: return "datetime";
	}
	return "unknown";
}

inline Schema LoadSchemaCsv(std::istream &in, char delimiter = ',') {
	CSVReader reader(in, delimiter);

	Schema schema;
	schema.reserve(64);

	std::unordered_set<std::string> seen_names;
	std::size_t line = 0;

	while (true) {
		std::optional<Row> row = reader.ReadNext();
		if (!row.has_value()) break;
		++line;

		bool all_empty = true;
		for (const auto &f : *row) {
			if (!utils::Trim(f).empty()) { all_empty = false; break; }
		}
		if (all_empty) continue;

		if (row->size() < 2) throw std::runtime_error("schema.csv parse error");

		std::string name(utils::Trim((*row)[0]));
		std::string type_str(utils::Trim((*row)[1]));

		if (name.empty() || !seen_names.insert(name).second) {
			throw std::runtime_error("schema.csv parse error");
		}

		schema.push_back(ColumnSchema{std::move(name), ParseColumnType(type_str)});
	}

	if (schema.empty()) throw std::runtime_error("schema.csv parse error: schema is empty");
	return schema;
}

inline void SaveSchemaCsv(std::ostream &out, const Schema &schema, char delimiter = ',') {
	if (schema.empty()) throw std::runtime_error("cannot write empty schema");

	CSVWriter writer(out, delimiter);
	for (const auto &col : schema) {
		if (col.name.empty()) throw std::runtime_error("cannot write schema: column name is empty");
		if (!writer.WriteNext({col.name, ToString(col.type)})) {
			throw std::runtime_error("failed to write schema row");
		}
	}
}
