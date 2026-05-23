#include "expr.h"

#include <stdexcept>
#include <type_traits>

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

std::size_t ColumnIndexByName(const Schema &s, std::string_view name) {
	for (std::size_t i = 0; i < s.size(); ++i) {
		if (s[i].name == name) return i;
	}
	throw std::runtime_error("column not found: " + std::string(name));
}

namespace {

template<class Src, class Dst>
std::vector<Dst> GatherCast(const std::vector<Src> &src,
                            const std::vector<std::uint32_t> *sel) {
	std::vector<Dst> out;
	if (sel == nullptr) {
		out.resize(src.size());
		for (std::size_t i = 0; i < src.size(); ++i) out[i] = static_cast<Dst>(src[i]);
	} else {
		out.resize(sel->size());
		for (std::size_t i = 0; i < sel->size(); ++i) out[i] = static_cast<Dst>(src[(*sel)[i]]);
	}
	return out;
}

template<class Dst>
EvalCol GatherNumeric(const DataVector &col, const std::vector<std::uint32_t> *sel) {
	return std::visit([&](const auto &v) -> EvalCol {
		using V = std::decay_t<decltype(v)>;
		using SrcT = V::value_type;
		if constexpr (std::is_arithmetic_v<SrcT>) {
			return GatherCast<SrcT, Dst>(v, sel);
		} else {
			throw std::runtime_error("expr: arithmetic gather on non-numeric column");
		}
	}, col);
}

EvalCol GatherString(const DataVector &col, const std::vector<std::uint32_t> *sel) {
	const auto &src = std::get<std::vector<std::string>>(col);
	std::vector<std::string> out;
	if (sel == nullptr) {
		out = src;
	} else {
		out.reserve(sel->size());
		for (auto i : *sel) out.push_back(src[i]);
	}
	return out;
}

class ColumnExpr final : public Expr {
public:
	ColumnExpr(std::size_t idx, EvalType result_type)
		: idx_(idx), result_type_(result_type) {}

	EvalType result_type() const override { return result_type_; }

	EvalCol eval(const EvalContext &ctx) const override {
		const auto &col = ctx.batch->GetColumn(idx_);
		switch (result_type_) {
			case EvalType::I64:
			case EvalType::Date:
			case EvalType::DateTime: return GatherNumeric<std::int64_t>(col, ctx.sel);
			case EvalType::U64:      return GatherNumeric<std::uint64_t>(col, ctx.sel);
			case EvalType::F64:      return GatherNumeric<double>(col, ctx.sel);
			case EvalType::Str:      return GatherString(col, ctx.sel);
			case EvalType::Bool:
				throw std::runtime_error("expr: bool column read not supported");
		}
		throw std::runtime_error("ColumnExpr: bad result_type");
	}

private:
	std::size_t idx_;
	EvalType result_type_;
};

template<class T>
class ConstExpr final : public Expr {
public:
	ConstExpr(T v, EvalType t) : v_(std::move(v)), t_(t) {}
	EvalType result_type() const override { return t_; }
	EvalCol eval(const EvalContext &ctx) const override {
		return std::vector<T>(ctx.rows(), v_);
	}

private:
	T v_;
	EvalType t_;
};

}

ExprPtr MakeColumn(const Schema &s, std::size_t idx) {
	if (idx >= s.size()) throw std::runtime_error("MakeColumn: idx out of range");
	return std::make_unique<ColumnExpr>(idx, DataTypeToEvalType(s[idx].type));
}

ExprPtr MakeColumnByName(const Schema &s, std::string_view name) {
	return MakeColumn(s, ColumnIndexByName(s, name));
}

ExprPtr MakeConstI64(std::int64_t v) {
	return std::make_unique<ConstExpr<std::int64_t>>(v, EvalType::I64);
}

}
