#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "batch.h"
#include "columnar_reader.h"
#include "csvwriter.h"
#include "expr.h"
#include "filter.h"
#include "hash_aggregate.h"
#include "operator.h"
#include "project.h"
#include "scan.h"
#include "schema.h"
#include "sort.h"
#include "topk.h"

namespace fs = std::filesystem;

namespace {

using exec::ExprPtr;
using exec::GroupAggKind;
using exec::GroupAggSpec;
using KV = std::pair<std::string, ExprPtr>;

struct Plan {
	std::vector<std::unique_ptr<exec::Operator>> nodes;
	exec::Operator *root = nullptr;
};

void AddOp(Plan &p, std::unique_ptr<exec::Operator> op) {
	p.root = op.get();
	p.nodes.push_back(std::move(op));
}

void AddScan(Plan &p, columnar::ColumnarReader &rdr, std::vector<std::string> cols) {
	AddOp(p, std::make_unique<exec::Scan>(rdr, std::move(cols)));
}

void AddScanEmpty(Plan &p, columnar::ColumnarReader &rdr) {
	AddOp(p, std::make_unique<exec::Scan>(rdr, std::vector<std::size_t>{}));
}

void AddHashAgg(Plan &p, std::vector<KV> keys, std::vector<GroupAggSpec> aggs) {
	if (keys.empty()) {
		keys.emplace_back("__zero", exec::MakeConstI64(0));
	}
	AddOp(p, std::make_unique<exec::HashAggregate>(*p.root, std::move(keys), std::move(aggs)));
}

void AddFilter(Plan &p, exec::ExprPtr pred) {
	AddOp(p, std::make_unique<exec::Filter>(*p.root, std::move(pred)));
}

void AddProject(Plan &p, std::vector<KV> outs) {
	AddOp(p, std::make_unique<exec::Project>(*p.root, std::move(outs)));
}

void AddSort(Plan &p, std::vector<std::pair<ExprPtr, bool>> keys) {
	AddOp(p, std::make_unique<exec::Sort>(*p.root, std::move(keys)));
}

void AddTopK(Plan &p, std::vector<std::pair<ExprPtr, bool>> keys, std::size_t k) {
	AddOp(p, std::make_unique<exec::TopK>(*p.root, std::move(keys), k));
}

ExprPtr C(const Plan &p, std::string_view name) {
	return exec::MakeColumnByName(p.root->OutputSchema(), name);
}

template<class... Args>
std::vector<GroupAggSpec> Aggs(Args &&...args) {
	std::vector<GroupAggSpec> v;
	(v.push_back(std::forward<Args>(args)), ...);
	return v;
}

template<class... Args>
std::vector<KV> Cols(Args &&...args) {
	std::vector<KV> v;
	(v.push_back(std::forward<Args>(args)), ...);
	return v;
}

GroupAggSpec CountStar(std::string name) {
	return GroupAggSpec{std::move(name), GroupAggKind::CountStar, nullptr};
}
GroupAggSpec Sum(std::string name, ExprPtr e) {
	return GroupAggSpec{std::move(name), GroupAggKind::Sum, std::move(e)};
}
GroupAggSpec Min(std::string name, ExprPtr e) {
	return GroupAggSpec{std::move(name), GroupAggKind::Min, std::move(e)};
}
GroupAggSpec Max(std::string name, ExprPtr e) {
	return GroupAggSpec{std::move(name), GroupAggKind::Max, std::move(e)};
}
GroupAggSpec Avg(std::string name, ExprPtr e) {
	return GroupAggSpec{std::move(name), GroupAggKind::Avg, std::move(e)};
}
GroupAggSpec Distinct(std::string name, ExprPtr e) {
	return GroupAggSpec{std::move(name), GroupAggKind::CountDistinct, std::move(e)};
}

ExprPtr Ne(ExprPtr a, ExprPtr b) { return exec::MakeCompare(std::move(a), exec::CmpOp::Ne, std::move(b)); }
ExprPtr Eq(ExprPtr a, ExprPtr b) { return exec::MakeCompare(std::move(a), exec::CmpOp::Eq, std::move(b)); }

using SK = std::pair<ExprPtr, bool>;

template<class... Args>
std::vector<SK> SortKeys(Args &&...args) {
	std::vector<SK> v;
	(v.push_back(std::forward<Args>(args)), ...);
	return v;
}

// Q0: SELECT COUNT(*) FROM hits
Plan Q0(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScanEmpty(p, rdr);
	AddHashAgg(p, {}, Aggs(CountStar("count")));
	AddProject(p, Cols(KV{"count", C(p, "count")}));
	return p;
}

using QueryBuilder = Plan (*)(columnar::ColumnarReader &);

struct QueryEntry {
	const char *name;
	QueryBuilder build;
};

// Q1: SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0
Plan Q1(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"AdvEngineID"});
	AddFilter(p, Ne(C(p, "AdvEngineID"), exec::MakeConstI64(0)));
	AddHashAgg(p, {}, Aggs(CountStar("count")));
	AddProject(p, Cols(KV{"count", C(p, "count")}));
	return p;
}

// Q2: SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits
Plan Q2(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"AdvEngineID", "ResolutionWidth"});
	AddHashAgg(p, {}, Aggs(
		Sum("sum", C(p, "AdvEngineID")),
		CountStar("count"),
		Avg("avg", C(p, "ResolutionWidth"))));
	AddProject(p, Cols(
		KV{"sum", C(p, "sum")},
		KV{"count", C(p, "count")},
		KV{"avg", C(p, "avg")}));
	return p;
}

// Q3: SELECT AVG(UserID) FROM hits
Plan Q3(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID"});
	AddHashAgg(p, {}, Aggs(Avg("avg", C(p, "UserID"))));
	AddProject(p, Cols(KV{"avg", C(p, "avg")}));
	return p;
}

// Q4: SELECT COUNT(DISTINCT UserID) FROM hits
Plan Q4(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID"});
	AddHashAgg(p, {}, Aggs(Distinct("distinct", C(p, "UserID"))));
	AddProject(p, Cols(KV{"distinct", C(p, "distinct")}));
	return p;
}

// Q5: SELECT COUNT(DISTINCT SearchPhrase) FROM hits
Plan Q5(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase"});
	AddHashAgg(p, {}, Aggs(Distinct("distinct", C(p, "SearchPhrase"))));
	AddProject(p, Cols(KV{"distinct", C(p, "distinct")}));
	return p;
}

// Q6: SELECT MIN(EventDate), MAX(EventDate) FROM hits
Plan Q6(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"EventDate"});
	AddHashAgg(p, {}, Aggs(
		Min("min", C(p, "EventDate")),
		Max("max", C(p, "EventDate"))));
	AddProject(p, Cols(KV{"min", C(p, "min")}, KV{"max", C(p, "max")}));
	return p;
}

// Q7: SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0
//     GROUP BY AdvEngineID ORDER BY COUNT(*) DESC
Plan Q7(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"AdvEngineID"});
	AddFilter(p, Ne(C(p, "AdvEngineID"), exec::MakeConstI64(0)));
	AddHashAgg(p,
		Cols(KV{"AdvEngineID", C(p, "AdvEngineID")}),
		Aggs(CountStar("count")));
	AddSort(p, SortKeys(SK{C(p, "count"), false}));
	AddProject(p, Cols(
		KV{"AdvEngineID", C(p, "AdvEngineID")},
		KV{"count", C(p, "count")}));
	return p;
}

// Q8: SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits
//     GROUP BY RegionID ORDER BY u DESC LIMIT 10
Plan Q8(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"RegionID", "UserID"});
	AddHashAgg(p,
		Cols(KV{"RegionID", C(p, "RegionID")}),
		Aggs(Distinct("u", C(p, "UserID"))));
	AddTopK(p, SortKeys(SK{C(p, "u"), false}), 10);
	AddProject(p, Cols(
		KV{"RegionID", C(p, "RegionID")},
		KV{"u", C(p, "u")}));
	return p;
}

// Q9: SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth),
//     COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10
Plan Q9(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"RegionID", "AdvEngineID", "ResolutionWidth", "UserID"});
	AddHashAgg(p,
		Cols(KV{"RegionID", C(p, "RegionID")}),
		Aggs(
			Sum("sum_adv", C(p, "AdvEngineID")),
			CountStar("c"),
			Avg("avg_res", C(p, "ResolutionWidth")),
			Distinct("distinct_u", C(p, "UserID"))));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"RegionID", C(p, "RegionID")},
		KV{"sum_adv", C(p, "sum_adv")},
		KV{"c", C(p, "c")},
		KV{"avg_res", C(p, "avg_res")},
		KV{"distinct_u", C(p, "distinct_u")}));
	return p;
}

// Q10: SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits
//      WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10
Plan Q10(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"MobilePhoneModel", "UserID"});
	AddFilter(p, Ne(C(p, "MobilePhoneModel"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(KV{"MobilePhoneModel", C(p, "MobilePhoneModel")}),
		Aggs(Distinct("u", C(p, "UserID"))));
	AddTopK(p, SortKeys(SK{C(p, "u"), false}), 10);
	AddProject(p, Cols(
		KV{"MobilePhoneModel", C(p, "MobilePhoneModel")},
		KV{"u", C(p, "u")}));
	return p;
}

// Q11: SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits
//      WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10
Plan Q11(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"MobilePhone", "MobilePhoneModel", "UserID"});
	AddFilter(p, Ne(C(p, "MobilePhoneModel"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(
			KV{"MobilePhone", C(p, "MobilePhone")},
			KV{"MobilePhoneModel", C(p, "MobilePhoneModel")}),
		Aggs(Distinct("u", C(p, "UserID"))));
	AddTopK(p, SortKeys(SK{C(p, "u"), false}), 10);
	AddProject(p, Cols(
		KV{"MobilePhone", C(p, "MobilePhone")},
		KV{"MobilePhoneModel", C(p, "MobilePhoneModel")},
		KV{"u", C(p, "u")}));
	return p;
}

// Q12: SELECT SearchPhrase, COUNT(*) AS c FROM hits
//      WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
Plan Q12(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"SearchPhrase", C(p, "SearchPhrase")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q13: SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits
//      WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10
Plan Q13(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase", "UserID"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}),
		Aggs(Distinct("u", C(p, "UserID"))));
	AddTopK(p, SortKeys(SK{C(p, "u"), false}), 10);
	AddProject(p, Cols(
		KV{"SearchPhrase", C(p, "SearchPhrase")},
		KV{"u", C(p, "u")}));
	return p;
}

// Q14: SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits
//      WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10
Plan Q14(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchEngineID", "SearchPhrase"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(
			KV{"SearchEngineID", C(p, "SearchEngineID")},
			KV{"SearchPhrase", C(p, "SearchPhrase")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"SearchEngineID", C(p, "SearchEngineID")},
		KV{"SearchPhrase", C(p, "SearchPhrase")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q15: SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
Plan Q15(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID"});
	AddHashAgg(p,
		Cols(KV{"UserID", C(p, "UserID")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"UserID", C(p, "UserID")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q16: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
Plan Q16(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID", "SearchPhrase"});
	AddHashAgg(p,
		Cols(
			KV{"UserID", C(p, "UserID")},
			KV{"SearchPhrase", C(p, "SearchPhrase")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"UserID", C(p, "UserID")},
		KV{"SearchPhrase", C(p, "SearchPhrase")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q17: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase LIMIT 10
Plan Q17(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID", "SearchPhrase"});
	AddHashAgg(p,
		Cols(
			KV{"UserID", C(p, "UserID")},
			KV{"SearchPhrase", C(p, "SearchPhrase")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "UserID"), true}), 10);
	AddProject(p, Cols(
		KV{"UserID", C(p, "UserID")},
		KV{"SearchPhrase", C(p, "SearchPhrase")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q24: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10
Plan Q24(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase", "EventTime"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddTopK(p, SortKeys(SK{C(p, "EventTime"), true}), 10);
	AddProject(p, Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}));
	return p;
}

// Q25: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10
Plan Q25(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddTopK(p, SortKeys(SK{C(p, "SearchPhrase"), true}), 10);
	AddProject(p, Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}));
	return p;
}

// Q26: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10
Plan Q26(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase", "EventTime"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddTopK(p, SortKeys(
		SK{C(p, "EventTime"), true},
		SK{C(p, "SearchPhrase"), true}), 10);
	AddProject(p, Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}));
	return p;
}

// Q30: SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
//      FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10
Plan Q30(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase", "SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(
			KV{"SearchEngineID", C(p, "SearchEngineID")},
			KV{"ClientIP", C(p, "ClientIP")}),
		Aggs(
			CountStar("c"),
			Sum("sum_refresh", C(p, "IsRefresh")),
			Avg("avg_width", C(p, "ResolutionWidth"))));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"SearchEngineID", C(p, "SearchEngineID")},
		KV{"ClientIP", C(p, "ClientIP")},
		KV{"c", C(p, "c")},
		KV{"sum_refresh", C(p, "sum_refresh")},
		KV{"avg_width", C(p, "avg_width")}));
	return p;
}

// Q31: same as Q30 but WatchID instead of SearchEngineID
Plan Q31(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"SearchPhrase", "WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"});
	AddFilter(p, Ne(C(p, "SearchPhrase"), exec::MakeConstStr("")));
	AddHashAgg(p,
		Cols(
			KV{"WatchID", C(p, "WatchID")},
			KV{"ClientIP", C(p, "ClientIP")}),
		Aggs(
			CountStar("c"),
			Sum("sum_refresh", C(p, "IsRefresh")),
			Avg("avg_width", C(p, "ResolutionWidth"))));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"WatchID", C(p, "WatchID")},
		KV{"ClientIP", C(p, "ClientIP")},
		KV{"c", C(p, "c")},
		KV{"sum_refresh", C(p, "sum_refresh")},
		KV{"avg_width", C(p, "avg_width")}));
	return p;
}

// Q32: same as Q31 but without WHERE
Plan Q32(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"});
	AddHashAgg(p,
		Cols(
			KV{"WatchID", C(p, "WatchID")},
			KV{"ClientIP", C(p, "ClientIP")}),
		Aggs(
			CountStar("c"),
			Sum("sum_refresh", C(p, "IsRefresh")),
			Avg("avg_width", C(p, "ResolutionWidth"))));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"WatchID", C(p, "WatchID")},
		KV{"ClientIP", C(p, "ClientIP")},
		KV{"c", C(p, "c")},
		KV{"sum_refresh", C(p, "sum_refresh")},
		KV{"avg_width", C(p, "avg_width")}));
	return p;
}

// Q33: SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10
Plan Q33(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"URL"});
	AddHashAgg(p,
		Cols(KV{"URL", C(p, "URL")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(KV{"URL", C(p, "URL")}, KV{"c", C(p, "c")}));
	return p;
}

// Q34: SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10
Plan Q34(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"URL"});
	AddHashAgg(p,
		Cols(KV{"URL", C(p, "URL")}),
		Aggs(CountStar("c")));
	AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
	AddProject(p, Cols(
		KV{"const_1", exec::MakeConstI64(1)},
		KV{"URL", C(p, "URL")},
		KV{"c", C(p, "c")}));
	return p;
}

// Q19: SELECT UserID FROM hits WHERE UserID = 435090932899640449
Plan Q19(columnar::ColumnarReader &rdr) {
	Plan p;
	AddScan(p, rdr, {"UserID"});
	AddFilter(p, Eq(C(p, "UserID"), exec::MakeConstI64(435090932899640449LL)));
	AddProject(p, Cols(KV{"UserID", C(p, "UserID")}));
	return p;
}

const std::vector<QueryEntry> kQueries = {
	{"Q0", &Q0},
	{"Q1", &Q1},
	{"Q2", &Q2},
	{"Q3", &Q3},
	{"Q4", &Q4},
	{"Q5", &Q5},
	{"Q6", &Q6},
	{"Q7", &Q7},
	{"Q8", &Q8},
	{"Q9", &Q9},
	{"Q10", &Q10},
	{"Q11", &Q11},
	{"Q12", &Q12},
	{"Q13", &Q13},
	{"Q14", &Q14},
	{"Q15", &Q15},
	{"Q16", &Q16},
	{"Q17", &Q17},
	{"Q19", &Q19},
	{"Q24", &Q24},
	{"Q25", &Q25},
	{"Q26", &Q26},
	{"Q30", &Q30},
	{"Q31", &Q31},
	{"Q32", &Q32},
	{"Q33", &Q33},
	{"Q34", &Q34},
};

std::string CellToString(const DataVector &col, std::size_t row) {
	return std::visit([&](const auto &v) -> std::string {
		using T = typename std::decay_t<decltype(v)>::value_type;
		if constexpr (std::is_same_v<T, std::string>) return v[row];
		else return std::to_string(v[row]);
	}, col);
}

void WritePlanToCsv(exec::Operator &root, const fs::path &path) {
	std::ofstream out(path);
	if (!out) throw std::runtime_error("cannot open output: " + path.string());
	CSVWriter writer(out);

	bool wrote_header = false;
	while (auto eb = root.Next()) {
		const Batch &b = *eb->batch;
		if (!wrote_header) {
			std::vector<std::string> hdr;
			for (const auto &c : b.GetSchema()) hdr.push_back(c.name);
			writer.WriteNext(hdr);
			wrote_header = true;
		}
		for (std::size_t r = 0; r < b.RowCount(); ++r) {
			std::vector<std::string> row;
			for (std::size_t c = 0; c < b.ColCount(); ++c) {
				row.push_back(CellToString(b.GetColumn(c), r));
			}
			writer.WriteNext(row);
		}
	}
	out.flush();
}

std::string FormatIndex(std::size_t i) {
	std::string s = std::to_string(i);
	if (s.size() < 2) s = "0" + s;
	return s;
}

}

int main(int argc, char **argv) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " <hits.columnar> <output_dir>\n";
		return 1;
	}
	try {
		columnar::ColumnarReader rdr(argv[1]);
		const fs::path output_dir = argv[2];
		fs::create_directories(output_dir);

		std::cout << "file=" << argv[1]
		          << " columns=" << rdr.GetSchema().size()
		          << " batches=" << rdr.NumBatches() << "\n\n";

		for (std::size_t i = 0; i < kQueries.size(); ++i) {
			const auto &q = kQueries[i];
			const auto t0 = std::chrono::steady_clock::now();
			try {
				Plan plan = q.build(rdr);
				const fs::path csv_path = output_dir / ("q" + FormatIndex(i) + ".csv");
				WritePlanToCsv(*plan.root, csv_path);

				const double secs = std::chrono::duration<double>(
					std::chrono::steady_clock::now() - t0).count();
				std::cout << "[" << q.name << "] " << std::fixed << std::setprecision(3)
				          << secs << "s OK -> " << csv_path.filename().string() << "\n";
			} catch (const std::exception &e) {
				std::cout << "[" << q.name << "] FAIL: " << e.what() << "\n";
			}
		}
		return 0;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
