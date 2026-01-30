#include "csvwriter.h"

#include <ostream>

CSVWriter::CSVWriter(std::ostream &output, char delimiter)
	: out_(output), delim_(delimiter) {
}


bool CSVWriter::NeedsQuoting(const std::string &s, char delimiter) {
	for (char c: s) {
		if (c == '"' || c == delimiter || c == '\n' || c == '\r') {
			return true;
		}
	}
	return false;
}

std::string CSVWriter::EscapeField(const std::string &s) {
	std::string res;
	res.reserve(s.size());
	for (char c: s) {
		if (c == '"') res.push_back('"');
		res.push_back(c);
	}
	return res;
}

bool CSVWriter::WriteNext(const std::vector<std::string> &fields) {
	if (!out_) return false;

	for (std::size_t i = 0; i < fields.size(); ++i) {
		if (i != 0) out_.put(delim_);

		const std::string &f = fields[i];
		if (NeedsQuoting(f, delim_)) {
			out_.put('"');
			out_ << EscapeField(f);
			out_.put('"');
		} else {
			out_ << f;
		}

		if (!out_) return false;
	}

	out_ << lineEnding_;
	return static_cast<bool>(out_);
}
