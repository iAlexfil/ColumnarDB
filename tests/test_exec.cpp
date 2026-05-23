#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "batch.h"
#include "columnar_reader.h"
#include "columnar_writer.h"
#include "expr.h"
#include "hash_aggregate.h"
#include "project.h"
#include "scan.h"
#include "schema.h"
#include "utils/utils.h"

namespace fs = std::filesystem;

namespace {

fs::path TmpPath(const std::string &name) {
	auto p = fs::temp_directory_path() / ("colDB_exec_" + name + ".columnar");
	std::error_code ec;
	fs::remove(p, ec);
	return p;
}

void WriteSampleFile(const fs::path &p,
                     const std::vector<std::int32_t> &a,
                     const std::vector<std::int64_t> &b,
                     const std::vector<std::string> &c,
                     std::size_t batch_size) {
	Schema schema = {
		{"a", DataType::Int32},
		{"b", DataType::Int64},
		{"c", DataType::String},
	};
	columnar::ColumnarWriter w(p, schema);
	std::size_t i = 0;
	while (i < a.size()) {
		const std::size_t take = std::min(batch_size, a.size() - i);
		Batch batch(schema);
		batch.Reserve(take);
		auto &va = std::get<std::vector<std::int32_t>>(batch.GetColumn(0));
		auto &vb = std::get<std::vector<std::int64_t>>(batch.GetColumn(1));
		auto &vc = std::get<std::vector<std::string>>(batch.GetColumn(2));
		for (std::size_t k = 0; k < take; ++k) {
			va.push_back(a[i + k]);
			vb.push_back(b[i + k]);
			vc.push_back(c[i + k]);
		}
		batch.SetRowCount(take);
		w.WriteBatch(batch);
		i += take;
	}
	w.Finish();
}

}

TEST(ExecScan, IteratesAllBatches) {
	const auto p = TmpPath("scan");
	WriteSampleFile(p, {1,2,3,4,5}, {10,20,30,40,50}, {"a","b","c","d","e"}, 2);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::size_t total = 0;
	std::size_t calls = 0;
	while (auto eb = scan.Next()) {
		++calls;
		total += eb->size();
	}
	EXPECT_EQ(total, 5u);
	EXPECT_EQ(calls, 3u);
}

TEST(ExecScan, ProjectionByName) {
	const auto p = TmpPath("scan_proj_name");
	WriteSampleFile(p, {1,2,3,4}, {10,20,30,40}, {"a","b","c","d"}, 2);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"b"});

	ASSERT_EQ(scan.OutputSchema().size(), 1u);
	EXPECT_EQ(scan.OutputSchema()[0].name, "b");

	std::vector<std::int64_t> all;
	while (auto eb = scan.Next()) {
		const auto &col = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
		all.insert(all.end(), col.begin(), col.end());
	}
	EXPECT_EQ(all, (std::vector<std::int64_t>{10,20,30,40}));
}

TEST(ExecScan, EmptyProjectionGivesRowCountOnly) {
	const auto p = TmpPath("scan_proj_empty");
	WriteSampleFile(p, {1,2,3,4,5,6,7}, {0,0,0,0,0,0,0}, {"","","","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::size_t>{});

	EXPECT_EQ(scan.OutputSchema().size(), 0u);

	std::size_t total = 0;
	while (auto eb = scan.Next()) {
		EXPECT_EQ(eb->batch->ColCount(), 0u);
		total += eb->batch->RowCount();
	}
	EXPECT_EQ(total, 7u);
}

TEST(ExecQ0, CountStarSmall) {
	const auto p = TmpPath("q0_small");
	WriteSampleFile(p, {1,2,3,4,5}, {10,20,30,40,50}, {"a","b","c","d","e"}, 2);

	columnar::ColumnarReader rdr(p);

	exec::Scan scan(rdr, std::vector<std::size_t>{});

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));

	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back(exec::GroupAggSpec{"count", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));

	std::vector<std::pair<std::string, exec::ExprPtr>> proj_outs;
	proj_outs.emplace_back("count", exec::MakeColumnByName(ha.OutputSchema(), "count"));

	exec::Project proj(ha, std::move(proj_outs));

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 1u);

	const auto &col = std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(col[0], 5u);

	EXPECT_FALSE(proj.Next().has_value());
}

TEST(ExecQ0, CountStarEmptyFile) {
	const auto p = TmpPath("q0_empty");
	Schema schema = {{"a", DataType::Int32}};
	columnar::ColumnarWriter w(p, schema);
	w.Finish();

	columnar::ColumnarReader rdr(p);

	exec::Scan scan(rdr, std::vector<std::size_t>{});

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));

	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back(exec::GroupAggSpec{"count", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));

	auto eb = ha.Next();
	ASSERT_TRUE(eb.has_value());
	EXPECT_EQ(eb->batch->RowCount(), 0u);
}

TEST(ExecQ0, CountStarMultiBatch) {
	const auto p = TmpPath("q0_multi");
	std::vector<std::int32_t> a(1000);
	std::vector<std::int64_t> b(1000, 0);
	std::vector<std::string> c(1000, "x");
	for (int i = 0; i < 1000; ++i) a[i] = i;

	WriteSampleFile(p, a, b, c, 100);

	columnar::ColumnarReader rdr(p);

	exec::Scan scan(rdr, std::vector<std::size_t>{});

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));

	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back(exec::GroupAggSpec{"count", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));

	std::vector<std::pair<std::string, exec::ExprPtr>> proj_outs;
	proj_outs.emplace_back("count", exec::MakeColumnByName(ha.OutputSchema(), "count"));

	exec::Project proj(ha, std::move(proj_outs));

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &col = std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(col[0], 1000u);
}
