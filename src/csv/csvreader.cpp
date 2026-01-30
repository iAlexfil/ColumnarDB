#include "csvreader.h"

#include <istream>
#include <stdexcept>

CSVReader::CSVReader(std::istream &input, char delimiter)
	: in_(input), delim_(delimiter) {
}

std::optional<std::vector<std::string> > CSVReader::ReadNext() {
	std::vector<std::string> fields;
	std::string field;
	bool inQuotes = false;
	bool started = false;

	while (true) {
		int ci = in_.get();

		if (ci == EOF) {
			if (!started) return std::nullopt;
			if (inQuotes) {
				throw std::runtime_error("csv syntax error");
			}
			fields.push_back(field);
			return fields;
		}

		char c = static_cast<char>(ci);
		started = true;

		if (inQuotes) {
			if (c == '"') {
				if (in_.peek() == '"') {
					in_.get();
					field.push_back('"');
				} else {
					inQuotes = false;
				}
			} else {
				field.push_back(c);
			}
		} else {
			if (c == '"') {
				inQuotes = true;
			} else if (c == delim_) {
				fields.push_back(field);
				field.clear();
			} else if (c == '\r') {
				if (in_.peek() == '\n') in_.get();
				fields.push_back(field);
				return fields;
			} else if (c == '\n') {
				fields.push_back(field);
				return fields;
			} else {
				field.push_back(c);
			}
		}
	}
}
