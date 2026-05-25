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

EvalType CommonNumericType(EvalType a, EvalType b) {
	if (a == EvalType::F64 || b == EvalType::F64) return EvalType::F64;
	if (a == b) return a;
	if (IsIntegerLike(a) && IsIntegerLike(b)) return EvalType::I64;
	if (a == EvalType::U64 && IsIntegerLike(b)) return EvalType::F64;
	if (b == EvalType::U64 && IsIntegerLike(a)) return EvalType::F64;
	throw std::runtime_error("expr: incompatible numeric types");
}

template<class T>
std::vector<T> CastEval(const EvalCol &c) {
	return std::visit([](const auto &v) -> std::vector<T> {
		using S = typename std::decay_t<decltype(v)>::value_type;
		if constexpr (std::is_arithmetic_v<S>) {
			std::vector<T> out(v.size());
			for (std::size_t i = 0; i < v.size(); ++i) out[i] = static_cast<T>(v[i]);
			return out;
		} else {
			throw std::runtime_error("expr: cast non-numeric to numeric");
		}
	}, c);
}

template<class T, class Cmp>
std::vector<std::uint8_t> CmpLoop(const std::vector<T> &l, const std::vector<T> &r, Cmp cmp) {
	std::vector<std::uint8_t> out(l.size());
	for (std::size_t i = 0; i < l.size(); ++i) out[i] = cmp(l[i], r[i]) ? 1 : 0;
	return out;
}

template<class T>
std::vector<std::uint8_t> DispatchCmp(CmpOp op, const std::vector<T> &l, const std::vector<T> &r) {
	switch (op) {
		case CmpOp::Eq: return CmpLoop(l, r, [](const T &a, const T &b) { return a == b; });
		case CmpOp::Ne: return CmpLoop(l, r, [](const T &a, const T &b) { return a != b; });
		case CmpOp::Lt: return CmpLoop(l, r, [](const T &a, const T &b) { return a <  b; });
		case CmpOp::Le: return CmpLoop(l, r, [](const T &a, const T &b) { return a <= b; });
		case CmpOp::Gt: return CmpLoop(l, r, [](const T &a, const T &b) { return a >  b; });
		case CmpOp::Ge: return CmpLoop(l, r, [](const T &a, const T &b) { return a >= b; });
	}
	throw std::runtime_error("DispatchCmp: bad op");
}

class CompareExpr final : public Expr {
public:
	CompareExpr(ExprPtr l, CmpOp op, ExprPtr r) : l_(std::move(l)), op_(op), r_(std::move(r)) {
		if (l_->result_type() == EvalType::Str && r_->result_type() == EvalType::Str) {
			str_ = true;
		} else {
			str_ = false;
			common_ = CommonNumericType(l_->result_type(), r_->result_type());
		}
	}

	EvalType result_type() const override { return EvalType::Bool; }

	EvalCol eval(const EvalContext &ctx) const override {
		EvalCol lc = l_->eval(ctx);
		EvalCol rc = r_->eval(ctx);
		if (str_) {
			return DispatchCmp(op_,
				std::get<std::vector<std::string>>(lc),
				std::get<std::vector<std::string>>(rc));
		}
		switch (common_) {
			case EvalType::I64:
			case EvalType::Date:
			case EvalType::DateTime:
				return DispatchCmp(op_, CastEval<std::int64_t>(lc), CastEval<std::int64_t>(rc));
			case EvalType::U64:
				return DispatchCmp(op_, CastEval<std::uint64_t>(lc), CastEval<std::uint64_t>(rc));
			case EvalType::F64:
				return DispatchCmp(op_, CastEval<double>(lc), CastEval<double>(rc));
			default:
				throw std::runtime_error("CompareExpr: bad common type");
		}
	}

private:
	ExprPtr l_, r_;
	CmpOp op_;
	bool str_ = false;
	EvalType common_ = EvalType::I64;
};

}

bool IsIntegerLike(EvalType t) {
	return t == EvalType::I64 || t == EvalType::Date || t == EvalType::DateTime;
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

ExprPtr MakeConstU64(std::uint64_t v) {
	return std::make_unique<ConstExpr<std::uint64_t>>(v, EvalType::U64);
}

ExprPtr MakeConstF64(double v) {
	return std::make_unique<ConstExpr<double>>(v, EvalType::F64);
}

ExprPtr MakeConstStr(std::string v) {
	return std::make_unique<ConstExpr<std::string>>(std::move(v), EvalType::Str);
}

ExprPtr MakeConstDate(std::int64_t days) {
	return std::make_unique<ConstExpr<std::int64_t>>(days, EvalType::Date);
}

ExprPtr MakeConstDateTime(std::int64_t seconds) {
	return std::make_unique<ConstExpr<std::int64_t>>(seconds, EvalType::DateTime);
}

ExprPtr MakeCompare(ExprPtr l, CmpOp op, ExprPtr r) {
	return std::make_unique<CompareExpr>(std::move(l), op, std::move(r));
}

}
