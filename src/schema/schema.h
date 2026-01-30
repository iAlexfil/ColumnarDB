#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>
#include <iosfwd>
#include "utils/utils.h"


struct ColumnSchema {
	std::string name;
	DataType type;
};

using Schema = std::vector<ColumnSchema>;

DataType ParseColumnType(std::string_view s);
std::string ToString(DataType t);

Schema LoadSchemaCsv(std::istream& in, char delimiter = ',');
void SaveSchemaCsv(std::ostream& out, const Schema& schema, char delimiter = ',');