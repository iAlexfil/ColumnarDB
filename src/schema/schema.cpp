#include "schema.h"

#include "csvreader.h"
#include "csvwriter.h"
#include "utils/utils.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>


std::string ToLowerAscii(std::string_view s) {
	std::string out;
	out.reserve(s.size());
	for (unsigned char ch: s) {
		out.push_back(static_cast<char>(std::tolower(ch)));
	}
	return out;
}


DataType ParseColumnType(std::string_view s) {
	s = utils::Trim(s);
	const std::string low = ToLowerAscii(s);

	if (low == "int64") return DataType::Int64;
	if (low == "string") return DataType::String;

	throw std::runtime_error("unknown column type: '" + std::string(s) + "'");
}

std::string ToString(DataType t) {
	switch (t) {
		case DataType::Int64: return "int64";
		case DataType::String: return "string";
	}
	return "unknown";
}

Schema LoadSchemaCsv(std::istream &in, char delimiter) {
	CSVReader reader(in, delimiter);

	Schema schema;
	schema.reserve(64);

	std::unordered_set<std::string> seen_names;
	size_t line = 0;

	while (true) {
		std::optional<Row> row = reader.ReadNext();
		if (!row.has_value()) break;

		++line;
		bool all_empty = true;
		for (const auto &f: *row) {
			if (!utils::Trim(f).empty()) {
				all_empty = false;
				break;
			}
		}
		if (all_empty) continue;

		if (row->size() != 2) {
			throw std::runtime_error("schema.csv parse error");
		}

		std::string name = std::string(utils::Trim((*row)[0]));
		std::string type_str = std::string(utils::Trim((*row)[1]));

		if (name.empty() || !seen_names.insert(name).second) {
			throw std::runtime_error("schema.csv parse error");
		}

		schema.push_back(ColumnSchema{std::move(name), ParseColumnType(type_str)});
	}

	if (schema.empty()) {
		throw std::runtime_error("schema.csv parse error: schema is empty");
	}

	return schema;
}

void SaveSchemaCsv(std::ostream &out, const Schema &schema, char delimiter) {
	if (schema.empty()) {
		throw std::runtime_error("cannot write empty schema");
	}

	CSVWriter writer(out, delimiter);

	for (const auto &col: schema) {
		if (col.name.empty()) {
			throw std::runtime_error("cannot write schema: column name is empty");
		}
		if (!writer.WriteNext({col.name, ToString(col.type)})) {
			throw std::runtime_error("failed to write schema row");
		}
	}
}
