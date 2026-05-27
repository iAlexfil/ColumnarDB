#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <cstdio>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "batch.h"
#include "columnar_reader.h"
#include "expr.h"
#include "filter.h"
#include "func.h"
#include "hash_aggregate.h"
#include "operator.h"
#include "project.h"
#include "scan.h"
#include "schema.h"
#include "sort.h"
#include "topk.h"
#include "utils/parse.h"

namespace fs = std::filesystem;

namespace {
	using exec::ExprPtr;
	using exec::GroupAggKind;
	using exec::GroupAggSpec;
	using KV = std::pair<std::string, ExprPtr>;

	struct Plan {
		std::vector<std::unique_ptr<exec::Operator> > nodes;
		exec::Operator *root = nullptr;
		std::unordered_map<std::string, DataType> format_hints;
	};

	void AddOp(Plan &p, std::unique_ptr<exec::Operator> op) {
		p.root = op.get();
		p.nodes.push_back(std::move(op));
	}

	void AddScan(Plan &p, columnar::ColumnarReader &rdr, std::vector<std::string> cols) {
		AddOp(p, std::make_unique<exec::Scan>(rdr, std::move(cols)));
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

	void AppendTiebreakers(Plan &p, std::vector<std::pair<ExprPtr, bool> > &keys) {
		const auto &sch = p.root->OutputSchema();
		for (const auto &col: sch) {
			keys.emplace_back(exec::MakeColumnByName(sch, col.name), true);
		}
	}

	void AddSort(Plan &p, std::vector<std::pair<ExprPtr, bool> > keys) {
		AppendTiebreakers(p, keys);
		AddOp(p, std::make_unique<exec::Sort>(*p.root, std::move(keys)));
	}

	void AddTopK(Plan &p, std::vector<std::pair<ExprPtr, bool> > keys, std::size_t k) {
		AppendTiebreakers(p, keys);
		AddOp(p, std::make_unique<exec::TopK>(*p.root, std::move(keys), k));
	}

	void AddSortLimitOffset(Plan &p, std::vector<std::pair<ExprPtr, bool> > keys,
	                        std::size_t limit, std::size_t offset) {
		AppendTiebreakers(p, keys);
		AddOp(p, std::make_unique<exec::Sort>(*p.root, std::move(keys), limit, offset));
	}

	ExprPtr C(const Plan &p, std::string_view name) {
		return exec::MakeColumnByName(p.root->OutputSchema(), name);
	}

	template<class... Args>
	std::vector<GroupAggSpec> Aggs(Args &&... args) {
		std::vector<GroupAggSpec> v;
		(v.push_back(std::forward<Args>(args)), ...);
		return v;
	}

	template<class... Args>
	std::vector<KV> Cols(Args &&... args) {
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
	ExprPtr Ge(ExprPtr a, ExprPtr b) { return exec::MakeCompare(std::move(a), exec::CmpOp::Ge, std::move(b)); }
	ExprPtr Le(ExprPtr a, ExprPtr b) { return exec::MakeCompare(std::move(a), exec::CmpOp::Le, std::move(b)); }
	ExprPtr Add(ExprPtr a, ExprPtr b) { return exec::MakeArith(std::move(a), exec::ArithOp::Add, std::move(b)); }
	ExprPtr Sub(ExprPtr a, ExprPtr b) { return exec::MakeArith(std::move(a), exec::ArithOp::Sub, std::move(b)); }
	ExprPtr Mul(ExprPtr a, ExprPtr b) { return exec::MakeArith(std::move(a), exec::ArithOp::Mul, std::move(b)); }

	template<class... Args>
	ExprPtr AndN(Args &&... args) {
		std::vector<ExprPtr> v;
		(v.push_back(std::forward<Args>(args)), ...);
		return exec::MakeLogical(exec::LogOp::And, std::move(v));
	}

	template<class... Args>
	ExprPtr InList(ExprPtr lhs, Args &&... consts) {
		std::vector<ExprPtr> v;
		(v.push_back(std::forward<Args>(consts)), ...);
		return exec::MakeInList(std::move(lhs), std::move(v));
	}

	ExprPtr Not1(ExprPtr a) {
		std::vector<ExprPtr> v;
		v.push_back(std::move(a));
		return exec::MakeLogical(exec::LogOp::Not, std::move(v));
	}

	ExprPtr Like(ExprPtr s, std::string pat) {
		std::vector<ExprPtr> v;
		v.push_back(std::move(s));
		v.push_back(exec::MakeConstStr(std::move(pat)));
		return exec::MakeFuncCall("like", std::move(v));
	}

	ExprPtr Length(ExprPtr s) {
		std::vector<ExprPtr> v;
		v.push_back(std::move(s));
		return exec::MakeFuncCall("length", std::move(v));
	}

	ExprPtr ExtractMinute(ExprPtr ts) {
		std::vector<ExprPtr> v;
		v.push_back(exec::MakeConstStr("minute"));
		v.push_back(std::move(ts));
		return exec::MakeFuncCall("extract", std::move(v));
	}

	ExprPtr DateTruncMinute(ExprPtr ts) {
		std::vector<ExprPtr> v;
		v.push_back(exec::MakeConstStr("minute"));
		v.push_back(std::move(ts));
		return exec::MakeFuncCall("date_trunc", std::move(v));
	}

	ExprPtr RegexpReplace(ExprPtr s, std::string pat, std::string repl) {
		std::vector<ExprPtr> v;
		v.push_back(std::move(s));
		v.push_back(exec::MakeConstStr(std::move(pat)));
		v.push_back(exec::MakeConstStr(std::move(repl)));
		return exec::MakeFuncCall("regexp_replace", std::move(v));
	}

	ExprPtr If(ExprPtr c, ExprPtr t, ExprPtr f) {
		return exec::MakeIf(std::move(c), std::move(t), std::move(f));
	}

	using SK = std::pair<ExprPtr, bool>;

	template<class... Args>
	std::vector<SK> SortKeys(Args &&... args) {
		std::vector<SK> v;
		(v.push_back(std::forward<Args>(args)), ...);
		return v;
	}

	// Q0: SELECT COUNT(*) FROM hits
	Plan Q0(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {});
		AddHashAgg(p, {}, Aggs(CountStar("count")));
		AddProject(p, Cols(KV{"count", C(p, "count")}));
		return p;
	}

	using QueryBuilder = Plan (*)(columnar::ColumnarReader &);

	struct QueryEntry {
		int index;
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
		p.format_hints = {{"min", DataType::Date}, {"max", DataType::Date}};
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

	// Q18: SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*)
	//      FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
	Plan Q18(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"UserID", "EventTime", "SearchPhrase"});
		AddProject(p, Cols(
			           KV{"UserID", C(p, "UserID")},
			           KV{"m", ExtractMinute(C(p, "EventTime"))},
			           KV{"SearchPhrase", C(p, "SearchPhrase")}));
		AddHashAgg(p,
		           Cols(
			           KV{"UserID", C(p, "UserID")},
			           KV{"m", C(p, "m")},
			           KV{"SearchPhrase", C(p, "SearchPhrase")}),
		           Aggs(CountStar("c")));
		AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
		AddProject(p, Cols(
			           KV{"UserID", C(p, "UserID")},
			           KV{"m", C(p, "m")},
			           KV{"SearchPhrase", C(p, "SearchPhrase")},
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

	// Q20: SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'
	Plan Q20(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"URL"});
		AddFilter(p, Like(C(p, "URL"), "%google%"));
		AddHashAgg(p, {}, Aggs(CountStar("c")));
		AddProject(p, Cols(KV{"c", C(p, "c")}));
		return p;
	}

	// Q21: SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits
	//      WHERE URL LIKE '%google%' AND SearchPhrase <> ''
	//      GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
	Plan Q21(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"URL", "SearchPhrase"});
		AddFilter(p, AndN(
			          Like(C(p, "URL"), "%google%"),
			          Ne(C(p, "SearchPhrase"), exec::MakeConstStr(""))));
		AddHashAgg(p,
		           Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}),
		           Aggs(Min("min_url", C(p, "URL")), CountStar("c")));
		AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
		AddProject(p, Cols(
			           KV{"SearchPhrase", C(p, "SearchPhrase")},
			           KV{"min_url", C(p, "min_url")},
			           KV{"c", C(p, "c")}));
		return p;
	}

	// Q22: SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
	//      FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
	//      GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
	Plan Q22(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"Title", "URL", "SearchPhrase", "UserID"});
		AddFilter(p, AndN(
			          Like(C(p, "Title"), "%Google%"),
			          Not1(Like(C(p, "URL"), "%.google.%")),
			          Ne(C(p, "SearchPhrase"), exec::MakeConstStr(""))));
		AddHashAgg(p,
		           Cols(KV{"SearchPhrase", C(p, "SearchPhrase")}),
		           Aggs(
			           Min("min_url", C(p, "URL")),
			           Min("min_title", C(p, "Title")),
			           CountStar("c"),
			           Distinct("distinct_u", C(p, "UserID"))));
		AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
		AddProject(p, Cols(
			           KV{"SearchPhrase", C(p, "SearchPhrase")},
			           KV{"min_url", C(p, "min_url")},
			           KV{"min_title", C(p, "min_title")},
			           KV{"c", C(p, "c")},
			           KV{"distinct_u", C(p, "distinct_u")}));
		return p;
	}

	// Q23: SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10
	//      Note: projecting subset of columns for demonstration
	Plan Q23(columnar::ColumnarReader &rdr) {
		Plan p;
		AddOp(p, std::make_unique<exec::Scan>(rdr));
		AddFilter(p, Like(C(p, "URL"), "%google%"));
		AddTopK(p, SortKeys(SK{C(p, "EventTime"), true}), 10);
		std::vector<KV> outs;
		const auto &sch = rdr.GetSchema();
		outs.reserve(sch.size());
		for (const auto &col: sch) {
			outs.emplace_back(col.name, C(p, col.name));
		}
		AddProject(p, std::move(outs));
		p.format_hints["EventTime"] = DataType::DateTime;
		p.format_hints["EventDate"] = DataType::Date;
		p.format_hints["ClientEventTime"] = DataType::DateTime;
		p.format_hints["LocalEventTime"] = DataType::DateTime;
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

	// Q27: SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits
	//      WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
	Plan Q27(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "URL"});
		AddFilter(p, Ne(C(p, "URL"), exec::MakeConstStr("")));
		AddHashAgg(p,
		           Cols(KV{"CounterID", C(p, "CounterID")}),
		           Aggs(
			           Avg("l", Length(C(p, "URL"))),
			           CountStar("c")));
		AddFilter(p, exec::MakeCompare(C(p, "c"), exec::CmpOp::Gt, exec::MakeConstI64(100000)));
		AddTopK(p, SortKeys(SK{C(p, "l"), false}), 25);
		AddProject(p, Cols(
			           KV{"CounterID", C(p, "CounterID")},
			           KV{"l", C(p, "l")},
			           KV{"c", C(p, "c")}));
		return p;
	}

	// Q28: SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k,
	//      AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits
	//      WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
	Plan Q28(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"Referer"});
		AddFilter(p, Ne(C(p, "Referer"), exec::MakeConstStr("")));
		AddProject(p, Cols(
			           KV{"k", RegexpReplace(C(p, "Referer"), R"(^https?://(?:www\.)?([^/]+)/.*$)", "\\1")},
			           KV{"ref_len", Length(C(p, "Referer"))},
			           KV{"Referer", C(p, "Referer")}));
		AddHashAgg(p,
		           Cols(KV{"k", C(p, "k")}),
		           Aggs(
			           Avg("l", C(p, "ref_len")),
			           CountStar("c"),
			           Min("min_ref", C(p, "Referer"))));
		AddFilter(p, exec::MakeCompare(C(p, "c"), exec::CmpOp::Gt, exec::MakeConstI64(100000)));
		AddTopK(p, SortKeys(SK{C(p, "l"), false}), 25);
		AddProject(p, Cols(
			           KV{"k", C(p, "k")},
			           KV{"l", C(p, "l")},
			           KV{"c", C(p, "c")},
			           KV{"min_ref", C(p, "min_ref")}));
		return p;
	}

	// Q29: SELECT SUM(ResolutionWidth), SUM(ResolutionWidth+1), ..., SUM(ResolutionWidth+89) FROM hits
	Plan Q29(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"ResolutionWidth"});
		AddHashAgg(p, {}, Aggs(
			           Sum("sum_width", C(p, "ResolutionWidth")),
			           CountStar("c")));
		std::vector<KV> outs;
		for (int i = 0; i < 90; ++i) {
			std::string name = "s" + std::to_string(i);
			if (i == 0) {
				outs.emplace_back(name, C(p, "sum_width"));
			} else {
				outs.emplace_back(name, Add(C(p, "sum_width"), Mul(C(p, "c"), exec::MakeConstI64(i))));
			}
		}
		AddProject(p, std::move(outs));
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

	// Q35: SELECT ClientIP, ClientIP-1, ClientIP-2, ClientIP-3, COUNT(*) AS c
	//      FROM hits GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 ORDER BY c DESC LIMIT 10
	Plan Q35(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"ClientIP"});
		AddHashAgg(p,
		           Cols(KV{"ClientIP", C(p, "ClientIP")}),
		           Aggs(CountStar("c")));
		AddTopK(p, SortKeys(SK{C(p, "c"), false}), 10);
		AddProject(p, Cols(
			           KV{"ClientIP", C(p, "ClientIP")},
			           KV{"ClientIP_1", Sub(C(p, "ClientIP"), exec::MakeConstI64(1))},
			           KV{"ClientIP_2", Sub(C(p, "ClientIP"), exec::MakeConstI64(2))},
			           KV{"ClientIP_3", Sub(C(p, "ClientIP"), exec::MakeConstI64(3))},
			           KV{"c", C(p, "c")}));
		return p;
	}

	ExprPtr CommonDateFilter(const Plan &p) {
		return AndN(
			Eq(C(p, "CounterID"), exec::MakeConstI64(62)),
			Ge(C(p, "EventDate"), exec::MakeConstDate(15887)),
			Le(C(p, "EventDate"), exec::MakeConstDate(15917)));
	}

	// Q36: SELECT URL, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID=62 AND EventDate>='2013-07-01' AND EventDate<='2013-07-31'
	//      AND DontCountHits=0 AND IsRefresh=0 AND URL<>'' GROUP BY URL ORDER BY PageViews DESC LIMIT 10
	Plan Q36(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "EventDate", "DontCountHits", "IsRefresh", "URL"});
		AddFilter(p, AndN(
			          CommonDateFilter(p),
			          Eq(C(p, "DontCountHits"), exec::MakeConstI64(0)),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          Ne(C(p, "URL"), exec::MakeConstStr(""))));
		AddHashAgg(p,
		           Cols(KV{"URL", C(p, "URL")}),
		           Aggs(CountStar("PageViews")));
		AddTopK(p, SortKeys(SK{C(p, "PageViews"), false}), 10);
		AddProject(p, Cols(KV{"URL", C(p, "URL")}, KV{"PageViews", C(p, "PageViews")}));
		return p;
	}

	// Q37: same as Q36 but Title instead of URL
	Plan Q37(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "EventDate", "DontCountHits", "IsRefresh", "Title"});
		AddFilter(p, AndN(
			          CommonDateFilter(p),
			          Eq(C(p, "DontCountHits"), exec::MakeConstI64(0)),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          Ne(C(p, "Title"), exec::MakeConstStr(""))));
		AddHashAgg(p,
		           Cols(KV{"Title", C(p, "Title")}),
		           Aggs(CountStar("PageViews")));
		AddTopK(p, SortKeys(SK{C(p, "PageViews"), false}), 10);
		AddProject(p, Cols(KV{"Title", C(p, "Title")}, KV{"PageViews", C(p, "PageViews")}));
		return p;
	}

	// Q38: SELECT URL, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID=62 AND EventDate>='2013-07-01' AND EventDate<='2013-07-31'
	//      AND IsRefresh=0 AND IsLink<>0 AND IsDownload=0
	//      GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
	Plan Q38(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload", "URL"});
		AddFilter(p, AndN(
			          CommonDateFilter(p),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          Ne(C(p, "IsLink"), exec::MakeConstI64(0)),
			          Eq(C(p, "IsDownload"), exec::MakeConstI64(0))));
		AddHashAgg(p,
		           Cols(KV{"URL", C(p, "URL")}),
		           Aggs(CountStar("PageViews")));
		AddSortLimitOffset(p, SortKeys(SK{C(p, "PageViews"), false}), 10, 1000);
		AddProject(p, Cols(KV{"URL", C(p, "URL")}, KV{"PageViews", C(p, "PageViews")}));
		return p;
	}

	// Q39: SELECT TraficSourceID, SearchEngineID, AdvEngineID,
	//      CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src,
	//      URL AS Dst, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0
	//      GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst
	//      ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
	Plan Q39(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {
			        "CounterID", "EventDate", "IsRefresh", "TraficSourceID",
			        "SearchEngineID", "AdvEngineID", "Referer", "URL"
		        });
		AddFilter(p, AndN(CommonDateFilter(p), Eq(C(p, "IsRefresh"), exec::MakeConstI64(0))));
		AddProject(p, Cols(
			           KV{"TraficSourceID", C(p, "TraficSourceID")},
			           KV{"SearchEngineID", C(p, "SearchEngineID")},
			           KV{"AdvEngineID", C(p, "AdvEngineID")},
			           KV{
				           "Src", If(
					           AndN(Eq(C(p, "SearchEngineID"), exec::MakeConstI64(0)),
					                Eq(C(p, "AdvEngineID"), exec::MakeConstI64(0))),
					           C(p, "Referer"),
					           exec::MakeConstStr(""))
			           },
			           KV{"Dst", C(p, "URL")}));
		AddHashAgg(p,
		           Cols(
			           KV{"TraficSourceID", C(p, "TraficSourceID")},
			           KV{"SearchEngineID", C(p, "SearchEngineID")},
			           KV{"AdvEngineID", C(p, "AdvEngineID")},
			           KV{"Src", C(p, "Src")},
			           KV{"Dst", C(p, "Dst")}),
		           Aggs(CountStar("PageViews")));
		AddSortLimitOffset(p, SortKeys(SK{C(p, "PageViews"), false}), 10, 1000);
		AddProject(p, Cols(
			           KV{"TraficSourceID", C(p, "TraficSourceID")},
			           KV{"SearchEngineID", C(p, "SearchEngineID")},
			           KV{"AdvEngineID", C(p, "AdvEngineID")},
			           KV{"Src", C(p, "Src")},
			           KV{"Dst", C(p, "Dst")},
			           KV{"PageViews", C(p, "PageViews")}));
		return p;
	}

	// Q40: SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID=62 AND EventDate>='2013-07-01' AND EventDate<='2013-07-31'
	//      AND IsRefresh=0 AND TraficSourceID IN (-1,6) AND RefererHash=3594120000172545465
	//      GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100
	Plan Q40(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "EventDate", "IsRefresh", "TraficSourceID", "RefererHash", "URLHash"});
		AddFilter(p, AndN(
			          CommonDateFilter(p),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          InList(C(p, "TraficSourceID"), exec::MakeConstI64(-1), exec::MakeConstI64(6)),
			          Eq(C(p, "RefererHash"), exec::MakeConstI64(3594120000172545465LL))));
		AddHashAgg(p,
		           Cols(KV{"URLHash", C(p, "URLHash")}, KV{"EventDate", C(p, "EventDate")}),
		           Aggs(CountStar("PageViews")));
		AddSortLimitOffset(p, SortKeys(SK{C(p, "PageViews"), false}), 10, 100);
		AddProject(p, Cols(
			           KV{"URLHash", C(p, "URLHash")},
			           KV{"EventDate", C(p, "EventDate")},
			           KV{"PageViews", C(p, "PageViews")}));
		p.format_hints = {{"EventDate", DataType::Date}};
		return p;
	}

	// Q41: SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID=62 AND EventDate>='2013-07-01' AND EventDate<='2013-07-31'
	//      AND IsRefresh=0 AND DontCountHits=0 AND URLHash=2868770270353813622
	//      GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
	Plan Q41(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {
			        "CounterID", "EventDate", "IsRefresh", "DontCountHits", "URLHash",
			        "WindowClientWidth", "WindowClientHeight"
		        });
		AddFilter(p, AndN(
			          CommonDateFilter(p),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          Eq(C(p, "DontCountHits"), exec::MakeConstI64(0)),
			          Eq(C(p, "URLHash"), exec::MakeConstI64(2868770270353813622LL))));
		AddHashAgg(p,
		           Cols(
			           KV{"WindowClientWidth", C(p, "WindowClientWidth")},
			           KV{"WindowClientHeight", C(p, "WindowClientHeight")}),
		           Aggs(CountStar("PageViews")));
		AddSortLimitOffset(p, SortKeys(SK{C(p, "PageViews"), false}), 10, 10000);
		AddProject(p, Cols(
			           KV{"WindowClientWidth", C(p, "WindowClientWidth")},
			           KV{"WindowClientHeight", C(p, "WindowClientHeight")},
			           KV{"PageViews", C(p, "PageViews")}));
		return p;
	}

	// Q42: SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits
	//      WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15'
	//      AND IsRefresh = 0 AND DontCountHits = 0
	//      GROUP BY DATE_TRUNC('minute', EventTime)
	//      ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000
	Plan Q42(columnar::ColumnarReader &rdr) {
		Plan p;
		AddScan(p, rdr, {"CounterID", "EventDate", "IsRefresh", "DontCountHits", "EventTime"});
		AddFilter(p, AndN(
			          Eq(C(p, "CounterID"), exec::MakeConstI64(62)),
			          Ge(C(p, "EventDate"), exec::MakeConstDate(15900)),
			          Le(C(p, "EventDate"), exec::MakeConstDate(15901)),
			          Eq(C(p, "IsRefresh"), exec::MakeConstI64(0)),
			          Eq(C(p, "DontCountHits"), exec::MakeConstI64(0))));
		AddProject(p, Cols(KV{"M", DateTruncMinute(C(p, "EventTime"))}));
		AddHashAgg(p,
		           Cols(KV{"M", C(p, "M")}),
		           Aggs(CountStar("PageViews")));
		AddSortLimitOffset(p, SortKeys(SK{C(p, "M"), true}), 10, 1000);
		AddProject(p, Cols(KV{"M", C(p, "M")}, KV{"PageViews", C(p, "PageViews")}));
		p.format_hints = {{"M", DataType::DateTime}};
		return p;
	}

	const std::vector<QueryEntry> kQueries = {
		{0, &Q0},
		{1, &Q1},
		{2, &Q2},
		{3, &Q3},
		{4, &Q4},
		{5, &Q5},
		{6, &Q6},
		{7, &Q7},
		{8, &Q8},
		{9, &Q9},
		{10, &Q10},
		{11, &Q11},
		{12, &Q12},
		{13, &Q13},
		{14, &Q14},
		{15, &Q15},
		{16, &Q16},
		{17, &Q17},
		{18, &Q18},
		{19, &Q19},
		{20, &Q20},
		{21, &Q21},
		{22, &Q22},
		{23, &Q23},
		{24, &Q24},
		{25, &Q25},
		{26, &Q26},
		{27, &Q27},
		{28, &Q28},
		{29, &Q29},
		{30, &Q30},
		{31, &Q31},
		{32, &Q32},
		{33, &Q33},
		{34, &Q34},
		{35, &Q35},
		{36, &Q36},
		{37, &Q37},
		{38, &Q38},
		{39, &Q39},
		{40, &Q40},
		{41, &Q41},
		{42, &Q42},
	};

	void WriteQuotedString(std::ostream &out, const std::string &s) {
		out.put('"');
		for (char c: s) {
			if (c == '"') out.put('"');
			out.put(c);
		}
		out.put('"');
	}

	void WriteCell(std::ostream &out, const DataVector &col, std::size_t row, DataType out_type) {
		std::visit([&](const auto &v) {
			using T = typename std::decay_t<decltype(v)>::value_type;
			if constexpr (std::is_same_v<T, std::string>) {
				WriteQuotedString(out, v[row]);
			} else if constexpr (std::is_floating_point_v<T>) {
				char buf[64];
				std::snprintf(buf, sizeof(buf), "%.15g", static_cast<double>(v[row]));
				out << buf;
			} else if constexpr (std::is_integral_v<T>) {
				if (out_type == DataType::Date) {
					char buf[utils::kDateBufSize];
					utils::FormatDate(static_cast<std::int32_t>(v[row]), buf);
					out.write(buf, utils::kDateBufSize);
				} else if (out_type == DataType::DateTime) {
					char buf[utils::kDateTimeBufSize];
					utils::FormatDateTime(static_cast<std::int64_t>(v[row]), buf);
					out.write(buf, utils::kDateTimeBufSize);
				} else {
					out << v[row];
				}
			}
		}, col);
	}

	void WritePlanToCsv(const Plan &plan, const fs::path &path) {
		std::ofstream out(path);
		if (!out) throw std::runtime_error("cannot open output: " + path.string());

		exec::Operator &root = *plan.root;
		std::vector<DataType> out_types;
		bool wrote_header = false;

		while (auto eb = root.Next()) {
			const Batch &b = *eb->batch;
			if (!wrote_header) {
				const auto &sch = b.GetSchema();
				out_types.resize(sch.size());
				for (std::size_t c = 0; c < sch.size(); ++c) {
					auto it = plan.format_hints.find(sch[c].name);
					out_types[c] = (it != plan.format_hints.end()) ? it->second : sch[c].type;
					if (c > 0) out.put(',');
					out << sch[c].name;
				}
				out.put('\n');
				wrote_header = true;
			}
			for (std::size_t r = 0; r < b.RowCount(); ++r) {
				for (std::size_t c = 0; c < b.ColCount(); ++c) {
					if (c > 0) out.put(',');
					WriteCell(out, b.GetColumn(c), r, out_types[c]);
				}
				out.put('\n');
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
	std::string input, schema, output_dir, queries_filter;

	for (int i = 1; i < argc; ++i) {
		std::string a = argv[i];
		auto next = [&]() -> std::string {
			if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
			return argv[++i];
		};
		if (a == "--input") input = next();
		else if (a == "--schema") schema = next();
		else if (a == "--output_dir") output_dir = next();
		else if (a.rfind("--queries=", 0) == 0) queries_filter = a.substr(10);
		else if (a == "--queries") queries_filter = next();
		else {
			std::cerr << "Unknown flag: " << a << "\n";
			return 1;
		}
	}

	if (input.empty() || output_dir.empty()) {
		std::cerr << "Usage: " << argv[0]
				<< " --input <hits.columnar> --output_dir <dir>"
				" [--schema <hits.schema>] [--queries N]\n";
		return 1;
	}

	std::set<int> only_queries;
	if (!queries_filter.empty()) {
		std::stringstream ss(queries_filter);
		std::string tok;
		while (std::getline(ss, tok, ',')) {
			if (!tok.empty()) only_queries.insert(std::stoi(tok));
		}
	}

	try {
		columnar::ColumnarReader rdr(input);
		fs::create_directories(output_dir);

		std::cout << "file=" << input
				<< " columns=" << rdr.GetSchema().size()
				<< " batches=" << rdr.NumBatches() << "\n\n";

		for (const auto &q: kQueries) {
			if (!only_queries.empty() && !only_queries.count(q.index)) continue;

			const auto t0 = std::chrono::steady_clock::now();
			try {
				Plan plan = q.build(rdr);
				const fs::path csv_path = fs::path(output_dir) / ("q" + FormatIndex(q.index) + ".csv");
				WritePlanToCsv(plan, csv_path);

				const double secs = std::chrono::duration<double>(
					std::chrono::steady_clock::now() - t0).count();
				std::cout << "[Q" << q.index << "] " << std::fixed << std::setprecision(3)
						<< secs << "s OK -> " << csv_path.filename().string() << "\n";
			} catch (const std::exception &e) {
				std::cout << "[Q" << q.index << "] FAIL: " << e.what() << "\n";
			}
		}
		return 0;
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 2;
	}
}
