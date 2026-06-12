#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "batch.h"
#include "columnar_reader.h"
#include "columnar_writer.h"
#include "expr.h"
#include "filter.h"
#include "func.h"
#include "hash_aggregate.h"
#include "project.h"
#include "scan.h"
#include "sort.h"
#include "topk.h"
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
		auto &vc = std::get<DictColumn>(batch.GetColumn(2));
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

TEST(ExecSort, SingleKeyAsc) {
	const auto p = TmpPath("sort_asc");
	WriteSampleFile(p, {5, 2, 4, 1, 3}, {50, 20, 40, 10, 30}, {"e","b","d","a","c"}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), true);
	exec::Sort sort(scan, std::move(keys));

	auto eb = sort.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &col = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(col, (std::vector<std::int32_t>{1, 2, 3, 4, 5}));
}

TEST(ExecSort, DescWithLimit) {
	const auto p = TmpPath("sort_desc_limit");
	WriteSampleFile(p, {5, 2, 4, 1, 3}, {0,0,0,0,0}, {"","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), false);
	exec::Sort sort(scan, std::move(keys), 3);

	auto eb = sort.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 3u);
	const auto &col = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(col, (std::vector<std::int32_t>{5, 4, 3}));
}

TEST(ExecTopK, Basic) {
	const auto p = TmpPath("topk_basic");
	WriteSampleFile(p, {5, 2, 8, 1, 3, 7, 4}, {50,20,80,10,30,70,40},
	                {"a","b","c","d","e","f","g"}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), true);
	exec::TopK top(scan, std::move(keys), 3);

	auto eb = top.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 3u);
	const auto &col = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(col, (std::vector<std::int32_t>{1, 2, 3}));
}

TEST(ExecTopK, AfterGroupBy) {
	const auto p = TmpPath("topk_groupby");
	WriteSampleFile(p,
		{1,1,1,2,2,3},
		{0,0,0,0,0,0},
		{"a","a","a","b","b","c"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> ha_keys;
	ha_keys.emplace_back("a", exec::MakeColumnByName(scan.OutputSchema(), "a"));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"cnt", exec::GroupAggKind::CountStar, nullptr});
	exec::HashAggregate ha(scan, std::move(ha_keys), std::move(aggs));

	std::vector<std::pair<exec::ExprPtr, bool>> sk;
	sk.emplace_back(exec::MakeColumnByName(ha.OutputSchema(), "cnt"), false);
	exec::TopK top(ha, std::move(sk), 2);

	auto eb = top.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 2u);
	const auto &keys = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
	const auto &cnts = std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(keys, (std::vector<std::int64_t>{1, 2}));
	EXPECT_EQ(cnts, (std::vector<std::uint64_t>{3, 2}));
}

TEST(ExecScan, ProjectionByIndex) {
	const auto p = TmpPath("scan_proj_idx");
	WriteSampleFile(p, {1,2,3}, {10,20,30}, {"a","b","c"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::size_t>{2, 0});

	ASSERT_EQ(scan.OutputSchema().size(), 2u);
	EXPECT_EQ(scan.OutputSchema()[0].name, "c");
	EXPECT_EQ(scan.OutputSchema()[1].name, "a");

	auto eb = scan.Next();
	ASSERT_TRUE(eb.has_value());
	EXPECT_EQ(eb->batch->ColCount(), 2u);
	const auto &c = std::get<DictColumn>(eb->batch->GetColumn(0));
	const auto &a = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(c, (std::vector<std::string>{"a","b","c"}));
	EXPECT_EQ(a, (std::vector<std::int32_t>{1,2,3}));
}

TEST(ExecFilter, CompareEq) {
	const auto p = TmpPath("filter_eq");
	WriteSampleFile(p, {1, 2, 3, 2, 1}, {0,0,0,0,0}, {"","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Eq,
		exec::MakeConstI64(2));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 2u);
}

TEST(ExecFilter, CompareGt) {
	const auto p = TmpPath("filter_gt");
	WriteSampleFile(p, {1, 5, 3, 7, 2}, {0,0,0,0,0}, {"","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Gt,
		exec::MakeConstI64(3));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 2u);
}

TEST(ExecFilter, StringCompare) {
	const auto p = TmpPath("filter_str");
	WriteSampleFile(p, {1,2,3,4}, {0,0,0,0}, {"x","y","x","z"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"c"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "c"),
		exec::CmpOp::Eq,
		exec::MakeConstStr("x"));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 2u);
}

TEST(ExecFilter, ChainedFilters) {
	const auto p = TmpPath("filter_chain");
	WriteSampleFile(p, {1,2,3,4,5,6}, {10,20,30,40,50,60}, {"","","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a", "b"});

	auto p1 = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Gt, exec::MakeConstI64(2));
	exec::Filter f1(scan, std::move(p1));

	auto p2 = exec::MakeCompare(
		exec::MakeColumnByName(f1.OutputSchema(), "b"),
		exec::CmpOp::Le, exec::MakeConstI64(50));
	exec::Filter f2(f1, std::move(p2));

	std::size_t total = 0;
	while (auto eb = f2.Next()) total += eb->size();
	EXPECT_EQ(total, 3u);
}

TEST(ExecSort, MultiKeyDescAsc) {
	const auto p = TmpPath("sort_multi");
	WriteSampleFile(p, {1, 2, 1, 2, 1}, {30, 10, 10, 30, 20},
	                {"a","b","c","d","e"}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), false);
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "b"), true);
	exec::Sort sort(scan, std::move(keys));

	auto eb = sort.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &a = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	const auto &b = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(a, (std::vector<std::int32_t>{2, 2, 1, 1, 1}));
	EXPECT_EQ(b, (std::vector<std::int64_t>{10, 30, 10, 20, 30}));
}

TEST(ExecSort, LimitAndOffset) {
	const auto p = TmpPath("sort_limit_offset");
	WriteSampleFile(p, {5,2,4,1,3,7,6}, {0,0,0,0,0,0,0},
	                {"a","b","c","d","e","f","g"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), true);
	exec::Sort sort(scan, std::move(keys), 3, 2);

	auto eb = sort.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &a = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(a, (std::vector<std::int32_t>{3, 4, 5}));
}

TEST(ExecTopK, KGreaterThanInput) {
	const auto p = TmpPath("topk_big_k");
	WriteSampleFile(p, {3,1,2}, {0,0,0}, {"","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);
	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), true);
	exec::TopK top(scan, std::move(keys), 100);

	auto eb = top.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 3u);
	const auto &a = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(a, (std::vector<std::int32_t>{1, 2, 3}));
}

TEST(ExecTopK, DescendingMultiKey) {
	const auto p = TmpPath("topk_desc_multi");
	WriteSampleFile(p, {1,2,1,2,1,2}, {30,10,20,40,10,30},
	                {"a","b","c","d","e","f"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<exec::ExprPtr, bool>> keys;
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "a"), false);
	keys.emplace_back(exec::MakeColumnByName(scan.OutputSchema(), "b"), true);
	exec::TopK top(scan, std::move(keys), 3);

	auto eb = top.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &a = std::get<std::vector<std::int32_t>>(eb->batch->GetColumn(0));
	const auto &b = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(a, (std::vector<std::int32_t>{2, 2, 2}));
	EXPECT_EQ(b, (std::vector<std::int64_t>{10, 30, 40}));
}

TEST(ExecAgg, GroupBySingleKey) {
	const auto p = TmpPath("agg_groupby");
	WriteSampleFile(p, {1, 2, 1, 2, 1}, {10, 20, 30, 40, 50},
	                {"x","y","x","y","x"}, 2);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> ha_keys;
	ha_keys.emplace_back("a", exec::MakeColumnByName(scan.OutputSchema(), "a"));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"cnt", exec::GroupAggKind::CountStar, nullptr});
	aggs.push_back({"sum_b", exec::GroupAggKind::Sum,
		exec::MakeColumnByName(scan.OutputSchema(), "b")});

	exec::HashAggregate ha(scan, std::move(ha_keys), std::move(aggs));

	auto eb = ha.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 2u);

	const auto &k = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
	const auto &cnt = std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(1));
	const auto &sm = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(2));

	std::map<std::int64_t, std::pair<std::uint64_t, std::int64_t>> got;
	for (std::size_t i = 0; i < k.size(); ++i) got[k[i]] = {cnt[i], sm[i]};

	EXPECT_EQ(got[1].first, 3u);
	EXPECT_EQ(got[1].second, 90);
	EXPECT_EQ(got[2].first, 2u);
	EXPECT_EQ(got[2].second, 60);
}

TEST(ExecAgg, GroupByMultiKey) {
	const auto p = TmpPath("agg_multikey");
	WriteSampleFile(p, {1, 2, 1, 2}, {0, 0, 0, 0},
	                {"x","x","y","y"}, 2);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> ha_keys;
	ha_keys.emplace_back("a", exec::MakeColumnByName(scan.OutputSchema(), "a"));
	ha_keys.emplace_back("c", exec::MakeColumnByName(scan.OutputSchema(), "c"));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"cnt", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(scan, std::move(ha_keys), std::move(aggs));

	auto eb = ha.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 4u);
}

TEST(ExecAgg, CountDistinctString) {
	const auto p = TmpPath("agg_cd_str");
	WriteSampleFile(p, {1,2,3,4,5,6}, {0,0,0,0,0,0},
	                {"a","b","a","c","b","a"}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"dist", exec::GroupAggKind::CountDistinct,
		exec::MakeColumnByName(scan.OutputSchema(), "c")});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));
	auto eb = ha.Next();
	ASSERT_TRUE(eb.has_value());
	EXPECT_EQ(std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(1))[0], 3u);
}

TEST(ExecProject, PassthroughAndConst) {
	const auto p = TmpPath("proj_const");
	WriteSampleFile(p, {10, 20, 30}, {0, 0, 0}, {"a","b","c"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	std::vector<std::pair<std::string, exec::ExprPtr>> outs;
	outs.emplace_back("val", exec::MakeColumnByName(scan.OutputSchema(), "a"));
	outs.emplace_back("one", exec::MakeConstI64(1));
	exec::Project proj(scan, std::move(outs));

	ASSERT_EQ(proj.OutputSchema().size(), 2u);
	EXPECT_EQ(proj.OutputSchema()[0].name, "val");
	EXPECT_EQ(proj.OutputSchema()[1].name, "one");

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &val = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
	const auto &one = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(val, (std::vector<std::int64_t>{10, 20, 30}));
	EXPECT_EQ(one, (std::vector<std::int64_t>{1, 1, 1}));
}

TEST(ExecExpr, ArithAddSub) {
	const auto p = TmpPath("arith");
	WriteSampleFile(p, {1,2,3,4}, {10,20,30,40}, {"","","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a", "b"});

	std::vector<std::pair<std::string, exec::ExprPtr>> outs;
	outs.emplace_back("a_plus_b",
		exec::MakeArith(
			exec::MakeColumnByName(scan.OutputSchema(), "a"),
			exec::ArithOp::Add,
			exec::MakeColumnByName(scan.OutputSchema(), "b")));
	outs.emplace_back("a_times_10",
		exec::MakeArith(
			exec::MakeColumnByName(scan.OutputSchema(), "a"),
			exec::ArithOp::Mul,
			exec::MakeConstI64(10)));
	exec::Project proj(scan, std::move(outs));

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &c0 = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
	const auto &c1 = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(1));
	EXPECT_EQ(c0, (std::vector<std::int64_t>{11, 22, 33, 44}));
	EXPECT_EQ(c1, (std::vector<std::int64_t>{10, 20, 30, 40}));
}

TEST(ExecExpr, LogicalAndOrNot) {
	const auto p = TmpPath("logical");
	WriteSampleFile(p, {1,2,3,4,5,6}, {10,20,30,40,50,60}, {"","","","","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a", "b"});

	std::vector<exec::ExprPtr> and_args;
	and_args.push_back(exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Gt, exec::MakeConstI64(2)));
	and_args.push_back(exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "b"),
		exec::CmpOp::Le, exec::MakeConstI64(50)));

	auto pred = exec::MakeLogical(exec::LogOp::And, std::move(and_args));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 3u);
}

TEST(ExecExpr, InListInt) {
	const auto p = TmpPath("inlist");
	WriteSampleFile(p, {1,2,3,4,5}, {0,0,0,0,0}, {"","","","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	std::vector<exec::ExprPtr> consts;
	consts.push_back(exec::MakeConstI64(2));
	consts.push_back(exec::MakeConstI64(4));
	auto pred = exec::MakeInList(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		std::move(consts));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 2u);
}

TEST(ExecExpr, NotPredicate) {
	const auto p = TmpPath("not_pred");
	WriteSampleFile(p, {1,2,3,4}, {0,0,0,0}, {"","","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	std::vector<exec::ExprPtr> not_args;
	not_args.push_back(exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Eq, exec::MakeConstI64(3)));
	auto pred = exec::MakeLogical(exec::LogOp::Not, std::move(not_args));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 3u);
}

TEST(ExecFunc, Length) {
	const auto p = TmpPath("fn_length");
	WriteSampleFile(p, {1,2,3}, {0,0,0}, {"a","hello","xy"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"c"});

	std::vector<exec::ExprPtr> args;
	args.push_back(exec::MakeColumnByName(scan.OutputSchema(), "c"));

	std::vector<std::pair<std::string, exec::ExprPtr>> outs;
	outs.emplace_back("len", exec::MakeFuncCall("length", std::move(args)));
	exec::Project proj(scan, std::move(outs));

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	const auto &out = std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(0));
	EXPECT_EQ(out, (std::vector<std::int64_t>{1, 5, 2}));
}

TEST(ExecFunc, LikePrefix) {
	const auto p = TmpPath("fn_like_prefix");
	WriteSampleFile(p, {1,2,3,4}, {0,0,0,0},
	                {"google.com", "yandex.ru", "google.co.uk", "ya.ru"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"c"});

	std::vector<exec::ExprPtr> args;
	args.push_back(exec::MakeColumnByName(scan.OutputSchema(), "c"));
	args.push_back(exec::MakeConstStr("google%"));

	auto pred = exec::MakeFuncCall("like", std::move(args));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 2u);
}

TEST(ExecFunc, LikeContains) {
	const auto p = TmpPath("fn_like_contains");
	WriteSampleFile(p, {1,2,3,4}, {0,0,0,0},
	                {"abcxyz", "abc", "xyz", "xabcx"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"c"});

	std::vector<exec::ExprPtr> args;
	args.push_back(exec::MakeColumnByName(scan.OutputSchema(), "c"));
	args.push_back(exec::MakeConstStr("%abc%"));

	auto pred = exec::MakeFuncCall("like", std::move(args));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 3u);
}

TEST(ExecFunc, NotLike) {
	const auto p = TmpPath("fn_not_like");
	WriteSampleFile(p, {1,2,3}, {0,0,0}, {"abc", "xyz", "ab"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"c"});

	std::vector<exec::ExprPtr> args;
	args.push_back(exec::MakeColumnByName(scan.OutputSchema(), "c"));
	args.push_back(exec::MakeConstStr("ab%"));
	auto like_expr = exec::MakeFuncCall("like", std::move(args));

	std::vector<exec::ExprPtr> not_args;
	not_args.push_back(std::move(like_expr));
	auto pred = exec::MakeLogical(exec::LogOp::Not, std::move(not_args));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 1u);
}

TEST(ExecHaving, FilterAfterHashAggregate) {
	const auto p = TmpPath("having");
	WriteSampleFile(p, {1,1,1,2,2,3}, {0,0,0,0,0,0},
	                {"a","a","a","b","b","c"}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("a", exec::MakeColumnByName(scan.OutputSchema(), "a"));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"cnt", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(ha.OutputSchema(), "cnt"),
		exec::CmpOp::Gt, exec::MakeConstI64(2));
	exec::Filter filter(ha, std::move(pred));

	auto eb = filter.Next();
	ASSERT_TRUE(eb.has_value());
	EXPECT_EQ(eb->size(), 1u);
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

TEST(ExecFilter, IntCompareNe) {
	const auto p = TmpPath("filter_ne");
	WriteSampleFile(p, {0, 1, 0, 2, 0, 3}, {0,0,0,0,0,0}, {"","","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Ne,
		exec::MakeConstI64(0));
	exec::Filter filter(scan, std::move(pred));

	std::size_t total = 0;
	while (auto eb = filter.Next()) total += eb->size();
	EXPECT_EQ(total, 3u);
}

TEST(ExecFilter, AllFilteredOut) {
	const auto p = TmpPath("filter_empty");
	WriteSampleFile(p, {0, 0, 0}, {0,0,0}, {"","",""}, 4);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Gt,
		exec::MakeConstI64(100));
	exec::Filter filter(scan, std::move(pred));

	EXPECT_FALSE(filter.Next().has_value());
}

TEST(ExecAgg, SumMinMaxAvgDistinct) {
	const auto p = TmpPath("agg_all");
	WriteSampleFile(p, {1,2,3,4,5}, {10,20,30,40,50}, {"x","y","x","y","z"}, 2);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr);

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));

	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"sum_b", exec::GroupAggKind::Sum,
		exec::MakeColumnByName(scan.OutputSchema(), "b")});
	aggs.push_back({"min_a", exec::GroupAggKind::Min,
		exec::MakeColumnByName(scan.OutputSchema(), "a")});
	aggs.push_back({"max_a", exec::GroupAggKind::Max,
		exec::MakeColumnByName(scan.OutputSchema(), "a")});
	aggs.push_back({"avg_b", exec::GroupAggKind::Avg,
		exec::MakeColumnByName(scan.OutputSchema(), "b")});
	aggs.push_back({"dist_c", exec::GroupAggKind::CountDistinct,
		exec::MakeColumnByName(scan.OutputSchema(), "c")});

	exec::HashAggregate ha(scan, std::move(keys), std::move(aggs));
	auto eb = ha.Next();
	ASSERT_TRUE(eb.has_value());
	ASSERT_EQ(eb->batch->RowCount(), 1u);

	EXPECT_EQ(std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(1))[0], 150);
	EXPECT_EQ(std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(2))[0], 1);
	EXPECT_EQ(std::get<std::vector<std::int64_t>>(eb->batch->GetColumn(3))[0], 5);
	EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(eb->batch->GetColumn(4))[0], 30.0);
	EXPECT_EQ(std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(5))[0], 3u);
}

TEST(ExecAgg, FilterThenCount) {
	const auto p = TmpPath("filter_count");
	WriteSampleFile(p, {0,1,0,2,0,3}, {10,20,30,40,50,60}, {"","","","","",""}, 3);

	columnar::ColumnarReader rdr(p);
	exec::Scan scan(rdr, std::vector<std::string>{"a"});

	auto pred = exec::MakeCompare(
		exec::MakeColumnByName(scan.OutputSchema(), "a"),
		exec::CmpOp::Ne, exec::MakeConstI64(0));
	exec::Filter filter(scan, std::move(pred));

	std::vector<std::pair<std::string, exec::ExprPtr>> keys;
	keys.emplace_back("__zero", exec::MakeConstI64(0));
	std::vector<exec::GroupAggSpec> aggs;
	aggs.push_back({"count", exec::GroupAggKind::CountStar, nullptr});

	exec::HashAggregate ha(filter, std::move(keys), std::move(aggs));

	std::vector<std::pair<std::string, exec::ExprPtr>> outs;
	outs.emplace_back("count", exec::MakeColumnByName(ha.OutputSchema(), "count"));
	exec::Project proj(ha, std::move(outs));

	auto eb = proj.Next();
	ASSERT_TRUE(eb.has_value());
	EXPECT_EQ(std::get<std::vector<std::uint64_t>>(eb->batch->GetColumn(0))[0], 3u);
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
