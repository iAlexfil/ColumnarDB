#include <gtest/gtest.h>

#include <cstdint>
#include <sstream>
#include <string>

#include "schema.h"
#include "utils/parse.h"
#include "utils/utils.h"

TEST(DataType, EnumValues) {
	EXPECT_EQ(static_cast<uint8_t>(DataType::Int64), 0);
	EXPECT_EQ(static_cast<uint8_t>(DataType::String), 1);
	EXPECT_EQ(static_cast<uint8_t>(DataType::Int8), 2);
	EXPECT_EQ(static_cast<uint8_t>(DataType::DateTime), 12);
}

TEST(ParseInteger, Int64Basic) {
	EXPECT_EQ(utils::ParseInteger<std::int64_t>("42", 1, "a"), 42);
	EXPECT_EQ(utils::ParseInteger<std::int64_t>("-100", 1, "a"), -100);
	EXPECT_EQ(utils::ParseInteger<std::int64_t>("  0  ", 1, "a"), 0);
}

TEST(ParseInteger, Int16Range) {
	EXPECT_EQ(utils::ParseInteger<std::int16_t>("32767", 1, "a"), 32767);
	EXPECT_EQ(utils::ParseInteger<std::int16_t>("-32768", 1, "a"), -32768);
	EXPECT_THROW(utils::ParseInteger<std::int16_t>("32768", 1, "a"), std::runtime_error);
}

TEST(ParseInteger, UInt8Range) {
	EXPECT_EQ(utils::ParseInteger<std::uint8_t>("0", 1, "a"), 0);
	EXPECT_EQ(utils::ParseInteger<std::uint8_t>("255", 1, "a"), 255);
	EXPECT_THROW(utils::ParseInteger<std::uint8_t>("256", 1, "a"), std::runtime_error);
}

TEST(ParseInteger, EmptyThrows) {
	EXPECT_THROW(utils::ParseInteger<std::int64_t>("", 1, "a"), std::runtime_error);
	EXPECT_THROW(utils::ParseInteger<std::int64_t>("   ", 1, "a"), std::runtime_error);
}

TEST(ParseInteger, GarbageThrows) {
	EXPECT_THROW(utils::ParseInteger<std::int64_t>("abc", 1, "a"), std::runtime_error);
	EXPECT_THROW(utils::ParseInteger<std::int64_t>("12x", 1, "a"), std::runtime_error);
}

TEST(ParseFloating, BasicFloat) {
	EXPECT_FLOAT_EQ(utils::ParseFloating<float>("3.14", 1, "a"), 3.14f);
	EXPECT_DOUBLE_EQ(utils::ParseFloating<double>("-1.5e2", 1, "a"), -150.0);
}

TEST(ParseFloating, EmptyThrows) {
	EXPECT_THROW(utils::ParseFloating<double>("", 1, "a"), std::runtime_error);
}

TEST(ParseDate, ValidDates) {
	std::int32_t d = utils::ParseDate("2013-07-15", 1, "d");
	EXPECT_EQ(d, 15901);

	std::int32_t epoch = utils::ParseDate("1970-01-01", 1, "d");
	EXPECT_EQ(epoch, 0);
}

TEST(ParseDate, InvalidFormat) {
	EXPECT_THROW(utils::ParseDate("2013/07/15", 1, "d"), std::runtime_error);
	EXPECT_THROW(utils::ParseDate("not-a-date", 1, "d"), std::runtime_error);
	EXPECT_THROW(utils::ParseDate("2013-13-01", 1, "d"), std::runtime_error);
}

TEST(ParseDateTime, ValidDateTimes) {
	std::int64_t dt = utils::ParseDateTime("1970-01-01 00:00:00", 1, "dt");
	EXPECT_EQ(dt, 0);

	std::int64_t dt2 = utils::ParseDateTime("1970-01-01 01:00:00", 1, "dt");
	EXPECT_EQ(dt2, 3600);

	std::int64_t dt3 = utils::ParseDateTime("1970-01-02 00:00:00", 1, "dt");
	EXPECT_EQ(dt3, 86400);
}

TEST(ParseDateTime, InvalidFormat) {
	EXPECT_THROW(utils::ParseDateTime("2013-07-15", 1, "dt"), std::runtime_error);
	EXPECT_THROW(utils::ParseDateTime("not-a-datetime-str", 1, "dt"), std::runtime_error);
}

TEST(FormatDate, RoundTrip) {
	char buf[10];
	utils::FormatDate(15901, buf);
	EXPECT_EQ(std::string(buf, 10), "2013-07-15");

	utils::FormatDate(0, buf);
	EXPECT_EQ(std::string(buf, 10), "1970-01-01");
}

TEST(FormatDateTime, RoundTrip) {
	char buf[19];
	utils::FormatDateTime(0, buf);
	EXPECT_EQ(std::string(buf, 19), "1970-01-01 00:00:00");

	utils::FormatDateTime(86400 + 3661, buf);
	EXPECT_EQ(std::string(buf, 19), "1970-01-02 01:01:01");
}

TEST(ParseColumnType, AllTypes) {
	EXPECT_EQ(ParseColumnType("int8"), DataType::Int8);
	EXPECT_EQ(ParseColumnType("INT16"), DataType::Int16);
	EXPECT_EQ(ParseColumnType("Int32"), DataType::Int32);
	EXPECT_EQ(ParseColumnType("int64"), DataType::Int64);
	EXPECT_EQ(ParseColumnType("uint8"), DataType::UInt8);
	EXPECT_EQ(ParseColumnType("UInt16"), DataType::UInt16);
	EXPECT_EQ(ParseColumnType("uint32"), DataType::UInt32);
	EXPECT_EQ(ParseColumnType("UINT64"), DataType::UInt64);
	EXPECT_EQ(ParseColumnType("float32"), DataType::Float32);
	EXPECT_EQ(ParseColumnType("float"), DataType::Float32);
	EXPECT_EQ(ParseColumnType("float64"), DataType::Float64);
	EXPECT_EQ(ParseColumnType("double"), DataType::Float64);
	EXPECT_EQ(ParseColumnType("string"), DataType::String);
	EXPECT_EQ(ParseColumnType("date"), DataType::Date);
	EXPECT_EQ(ParseColumnType("datetime"), DataType::DateTime);
	EXPECT_EQ(ParseColumnType("timestamp"), DataType::DateTime);
	EXPECT_EQ(ParseColumnType("char"), DataType::String);
}

TEST(ParseColumnType, UnknownThrows) {
	EXPECT_THROW(ParseColumnType("boolean"), std::runtime_error);
	EXPECT_THROW(ParseColumnType(""), std::runtime_error);
}

TEST(ToString, AllTypes) {
	EXPECT_EQ(ToString(DataType::Int64), "int64");
	EXPECT_EQ(ToString(DataType::String), "string");
	EXPECT_EQ(ToString(DataType::Date), "date");
	EXPECT_EQ(ToString(DataType::DateTime), "datetime");
	EXPECT_EQ(ToString(DataType::Float32), "float32");
}

TEST(LoadSchemaCsv, BasicTwoColumn) {
	std::istringstream in("id,int64\nname,string\n");
	Schema s = LoadSchemaCsv(in);
	ASSERT_EQ(s.size(), 2u);
	EXPECT_EQ(s[0].name, "id");
	EXPECT_EQ(s[0].type, DataType::Int64);
	EXPECT_EQ(s[1].name, "name");
	EXPECT_EQ(s[1].type, DataType::String);
}

TEST(LoadSchemaCsv, ThreeColumnFormat) {
	std::istringstream in("WatchID,int64,\nEventDate,date,\nTitle,string,\n");
	Schema s = LoadSchemaCsv(in);
	ASSERT_EQ(s.size(), 3u);
	EXPECT_EQ(s[0].type, DataType::Int64);
	EXPECT_EQ(s[1].type, DataType::Date);
	EXPECT_EQ(s[2].type, DataType::String);
}

TEST(LoadSchemaCsv, TimestampAlias) {
	std::istringstream in("ts,timestamp,\n");
	Schema s = LoadSchemaCsv(in);
	ASSERT_EQ(s.size(), 1u);
	EXPECT_EQ(s[0].type, DataType::DateTime);
}

TEST(LoadSchemaCsv, EmptyThrows) {
	std::istringstream in("");
	EXPECT_THROW(LoadSchemaCsv(in), std::runtime_error);
}

TEST(LoadSchemaCsv, DuplicateNameThrows) {
	std::istringstream in("a,int64\na,string\n");
	EXPECT_THROW(LoadSchemaCsv(in), std::runtime_error);
}

TEST(Trim, Basic) {
	EXPECT_EQ(utils::Trim("  hello  "), "hello");
	EXPECT_EQ(utils::Trim(""), "");
	EXPECT_EQ(utils::Trim("abc"), "abc");
}
