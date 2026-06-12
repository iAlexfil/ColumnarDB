#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <vector>

#include "csvreader.h"
#include "csvwriter.h"

TEST(CSVReaderTests, ReadsSimpleLine) {
    std::istringstream in("a,b,c\n");
    CSVReader r(in);

    auto row = r.ReadNext();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ((*row), (std::vector<std::string>{"a","b","c"}));

    EXPECT_FALSE(r.ReadNext().has_value());
}

TEST(CSVReaderTests, ReadsQuotedAndEscapedQuotes) {
    std::istringstream in("\"a\",\"b\"\"b\",\"c\"\n");
    CSVReader r(in);

    auto row = r.ReadNext();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ((*row), (std::vector<std::string>{"a","b\"b","c"}));
}

TEST(CSVReaderTests, ReadsDelimiterInsideQuotes) {
    std::istringstream in("\"a,b\",c\n");
    CSVReader r(in);

    auto row = r.ReadNext();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ((*row), (std::vector<std::string>{"a,b","c"}));
}

TEST(CSVReaderTests, ReadsNewlineInsideQuotes) {
    std::istringstream in("\"hello\nworld\",x\n");
    CSVReader r(in);

    auto row = r.ReadNext();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ((*row), (std::vector<std::string>{"hello\nworld","x"}));
}

TEST(CSVWriterTests, WritesSimpleLine) {
    std::ostringstream out;
    CSVWriter w(out);

    ASSERT_TRUE(w.WriteNext({"a","b","c"}));
    EXPECT_EQ(out.str(), "a,b,c\n");
}

TEST(CSVWriterTests, QuotesAndEscapesWhenNeeded) {
    std::ostringstream out;
    CSVWriter w(out);

    ASSERT_TRUE(w.WriteNext({"a,b", "He said \"hi\"", "x\ny"}));
    EXPECT_EQ(out.str(), "\"a,b\",\"He said \"\"hi\"\"\",\"x\ny\"\n");
}

TEST(CSVRoundTripTests, WriterThenReader) {
    std::ostringstream out;
    CSVWriter w(out);

    std::vector<std::string> original{"a,b", "b\"b", "hello\nworld", "plain"};
    ASSERT_TRUE(w.WriteNext(original));

    std::istringstream in(out.str());
    CSVReader r(in);

    auto row = r.ReadNext();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ((*row), original);
}
