#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "batch.h"
#include "schema.h"
#include "utils/utils.h"

namespace exec {

enum class EvalType : std::uint8_t { I64, U64, F64, Str, Bool, Date, DateTime };

using EvalCol = std::variant<
	std::vector<std::int64_t>,
	std::vector<std::uint64_t>,
	std::vector<double>,
	std::vector<std::string>,
	std::vector<std::uint8_t>
>;

std::size_t EvalColSize(const EvalCol &c);

DataType EvalTypeToDataType(EvalType t);
EvalType DataTypeToEvalType(DataType t);

struct EvalContext {
	const Batch *batch = nullptr;
	const std::vector<std::uint32_t> *sel = nullptr;

	std::size_t rows() const { return sel ? sel->size() : batch->RowCount(); }
};

class Expr {
public:
	virtual ~Expr() = default;
	virtual EvalType result_type() const = 0;
	virtual EvalCol eval(const EvalContext &) const = 0;
};

using ExprPtr = std::unique_ptr<Expr>;

enum class CmpOp   { Eq, Ne, Lt, Le, Gt, Ge };
enum class LogOp   { And, Or, Not };
enum class ArithOp { Add, Sub, Mul, Div, Mod };

ExprPtr MakeColumn(const Schema &s, std::size_t idx);
ExprPtr MakeColumnByName(const Schema &s, std::string_view name);

ExprPtr MakeConstI64(std::int64_t v);
ExprPtr MakeConstU64(std::uint64_t v);
ExprPtr MakeConstF64(double v);
ExprPtr MakeConstStr(std::string v);
ExprPtr MakeConstDate(std::int64_t days);
ExprPtr MakeConstDateTime(std::int64_t seconds);

ExprPtr MakeCompare(ExprPtr l, CmpOp op, ExprPtr r);
ExprPtr MakeLogical(LogOp op, std::vector<ExprPtr> args);
ExprPtr MakeArith(ExprPtr l, ArithOp op, ExprPtr r);
ExprPtr MakeInList(ExprPtr lhs, std::vector<ExprPtr> consts);

std::size_t ColumnIndexByName(const Schema &s, std::string_view name);
bool IsIntegerLike(EvalType t);

}
