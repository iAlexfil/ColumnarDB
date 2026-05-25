#include "expr.h"

#include "exprs/column.h"
#include "exprs/compare.h"
#include "exprs/logical.h"
#include "exprs/arith.h"
#include "exprs/in_list.h"

namespace exec {

std::size_t EvalColSize(const EvalCol &c) {
	return std::visit([](const auto &v) { return v.size(); }, c);
}

DataType EvalTypeToDataType(EvalType t) {
	switch (t) {
		case EvalType::I64:      return DataType::Int64;
		case EvalType::U64:      return DataType::UInt64;
		case EvalType::F64:      return DataType::Float64;
		case EvalType::Str:      return DataType::String;
		case EvalType::Bool:     return DataType::UInt8;
		case EvalType::Date:     return DataType::Int64;
		case EvalType::DateTime: return DataType::Int64;
	}
	throw std::runtime_error("EvalTypeToDataType: unknown");
}

EvalType DataTypeToEvalType(DataType t) {
	switch (t) {
		case DataType::Int8: case DataType::Int16:
		case DataType::Int32: case DataType::Int64: return EvalType::I64;
		case DataType::Date: return EvalType::Date;
		case DataType::DateTime: return EvalType::DateTime;
		case DataType::UInt8: case DataType::UInt16:
		case DataType::UInt32: case DataType::UInt64: return EvalType::U64;
		case DataType::Float32: case DataType::Float64: return EvalType::F64;
		case DataType::String: return EvalType::Str;
	}
	throw std::runtime_error("DataTypeToEvalType: unknown");
}

bool IsIntegerLike(EvalType t) {
	return t == EvalType::I64 || t == EvalType::Date || t == EvalType::DateTime;
}

std::size_t ColumnIndexByName(const Schema &s, std::string_view name) {
	for (std::size_t i = 0; i < s.size(); ++i) {
		if (s[i].name == name) return i;
	}
	throw std::runtime_error("column not found: " + std::string(name));
}

ExprPtr MakeColumn(const Schema &s, std::size_t idx) {
	if (idx >= s.size()) throw std::runtime_error("MakeColumn: idx out of range");
	return std::make_unique<detail::ColumnExpr>(idx, DataTypeToEvalType(s[idx].type));
}

ExprPtr MakeColumnByName(const Schema &s, std::string_view name) {
	return MakeColumn(s, ColumnIndexByName(s, name));
}

ExprPtr MakeConstI64(std::int64_t v) {
	return std::make_unique<detail::ConstExpr<std::int64_t>>(v, EvalType::I64);
}

ExprPtr MakeConstU64(std::uint64_t v) {
	return std::make_unique<detail::ConstExpr<std::uint64_t>>(v, EvalType::U64);
}

ExprPtr MakeConstF64(double v) {
	return std::make_unique<detail::ConstExpr<double>>(v, EvalType::F64);
}

ExprPtr MakeConstStr(std::string v) {
	return std::make_unique<detail::ConstExpr<std::string>>(std::move(v), EvalType::Str);
}

ExprPtr MakeConstDate(std::int64_t days) {
	return std::make_unique<detail::ConstExpr<std::int64_t>>(days, EvalType::Date);
}

ExprPtr MakeConstDateTime(std::int64_t seconds) {
	return std::make_unique<detail::ConstExpr<std::int64_t>>(seconds, EvalType::DateTime);
}

ExprPtr MakeCompare(ExprPtr l, CmpOp op, ExprPtr r) {
	return std::make_unique<detail::CompareExpr>(std::move(l), op, std::move(r));
}

ExprPtr MakeLogical(LogOp op, std::vector<ExprPtr> args) {
	return std::make_unique<detail::LogicalExpr>(op, std::move(args));
}

ExprPtr MakeArith(ExprPtr l, ArithOp op, ExprPtr r) {
	return std::make_unique<detail::ArithExpr>(std::move(l), op, std::move(r));
}

ExprPtr MakeInList(ExprPtr lhs, std::vector<ExprPtr> consts) {
	return std::make_unique<detail::InListExpr>(std::move(lhs), std::move(consts));
}

}
