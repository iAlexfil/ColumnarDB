#pragma once

#include <iosfwd>
#include <string>
#include <vector>

class CSVWriter {
public:
	explicit CSVWriter(std::ostream &output, char delimiter = ',');

	bool WriteNext(const std::vector<std::string> &fields);

private:
	static bool NeedsQuoting(const std::string &s, char delimiter);

	static std::string EscapeField(const std::string &s);

	std::ostream &out_;
	char delim_;
	std::string lineEnding_ = "\n";
};
