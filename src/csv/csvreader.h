#pragma once

#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

using Row = std::vector<std::string>;

class CSVReader {
public:
	explicit CSVReader(std::istream& input, char delimiter = ',');

	std::optional<Row> ReadNext();

private:
	std::istream& in_;
	char delim_;
};
