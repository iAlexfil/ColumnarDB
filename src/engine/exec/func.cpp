#include "func.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdint>
#include <regex>
#include <stdexcept>
#include <string>
#include <variant>

namespace exec {
	namespace {
		bool MatchTypes(const std::vector<EvalType> &want, const std::vector<EvalType> &got) {
			if (want.size() != got.size()) return false;
			for (std::size_t i = 0; i < want.size(); ++i) {
				if (want[i] == got[i]) continue;
				if (want[i] == EvalType::Date && got[i] == EvalType::DateTime) continue;
				return false;
			}
			return true;
		}

		const std::vector<std::string> &GetStr(const EvalCol &c) {
			return std::get<std::vector<std::string> >(c);
		}

		const std::vector<std::int64_t> &GetI64(const EvalCol &c) {
			return std::get<std::vector<std::int64_t> >(c);
		}

		bool AllSame(const std::vector<std::string> &v) {
			if (v.empty()) return true;
			for (std::size_t i = 1; i < v.size(); ++i) {
				if (v[i] != v[0]) return false;
			}
			return true;
		}

		class LikePattern {
		public:
			explicit LikePattern(const std::string &p) : pattern_(p) {
			}

			bool Match(std::string_view s) const {
				std::size_t i = 0, j = 0, star = SIZE_MAX, match = 0;
				while (i < s.size()) {
					if (j < pattern_.size() && (pattern_[j] == '_' || pattern_[j] == s[i])) {
						++i;
						++j;
					} else if (j < pattern_.size() && pattern_[j] == '%') {
						star = j++;
						match = i;
					} else if (star != SIZE_MAX) {
						j = star + 1;
						i = ++match;
					} else {
						return false;
					}
				}
				while (j < pattern_.size() && pattern_[j] == '%') ++j;
				return j == pattern_.size();
			}

		private:
			std::string pattern_;
		};

		EvalCol FnLength(const std::vector<EvalCol> &args, std::size_t n) {
			const auto &s = GetStr(args[0]);
			std::vector<std::int64_t> out(n);
			for (std::size_t i = 0; i < n; ++i) out[i] = static_cast<std::int64_t>(s[i].size());
			return out;
		}

		EvalCol FnLike(const std::vector<EvalCol> &args, std::size_t n) {
			const auto &s = GetStr(args[0]);
			const auto &p = GetStr(args[1]);
			std::vector<std::uint8_t> out(n);
			if (p.empty()) return out;

			if (AllSame(p)) {
				LikePattern pat(p[0]);
				for (std::size_t i = 0; i < n; ++i) out[i] = pat.Match(s[i]) ? 1 : 0;
			} else {
				for (std::size_t i = 0; i < n; ++i) {
					LikePattern pi(p[i]);
					out[i] = pi.Match(s[i]) ? 1 : 0;
				}
			}
			return out;
		}

		std::int64_t ExtractFromDT(const std::string &unit, std::int64_t secs) {
			auto sys = std::chrono::sys_seconds{std::chrono::seconds{secs}};
			auto days = std::chrono::floor<std::chrono::days>(sys);
			auto tod = sys - days;
			std::chrono::year_month_day ymd{days};
			if (unit == "year") return static_cast<int>(ymd.year());
			if (unit == "month") return static_cast<unsigned>(ymd.month());
			if (unit == "day") return static_cast<unsigned>(ymd.day());
			if (unit == "hour") return std::chrono::duration_cast<std::chrono::hours>(tod).count();
			if (unit == "minute") return std::chrono::duration_cast<std::chrono::minutes>(tod).count() % 60;
			if (unit == "second") return std::chrono::duration_cast<std::chrono::seconds>(tod).count() % 60;
			throw std::runtime_error("extract: unsupported unit '" + unit + "'");
		}

		EvalCol FnExtractDT(const std::vector<EvalCol> &args, std::size_t n) {
			const auto &u = GetStr(args[0]);
			const auto &v = GetI64(args[1]);
			std::vector<std::int64_t> out(n);
			if (u.empty()) return out;

			if (AllSame(u)) {
				const std::string unit = u[0];
				for (std::size_t i = 0; i < n; ++i) out[i] = ExtractFromDT(unit, v[i]);
			} else {
				for (std::size_t i = 0; i < n; ++i) out[i] = ExtractFromDT(u[i], v[i]);
			}
			return out;
		}

		std::int64_t TruncSecondsBy(const std::string &unit, std::int64_t t) {
			if (unit == "second" || unit == "seconds") return t;
			if (unit == "minute" || unit == "minutes") return (t / 60) * 60;
			if (unit == "hour" || unit == "hours") return (t / 3600) * 3600;
			if (unit == "day" || unit == "days") return (t / 86400) * 86400;
			throw std::runtime_error("date_trunc: unsupported unit '" + unit + "'");
		}

		EvalCol FnDateTrunc(const std::vector<EvalCol> &args, std::size_t n) {
			const auto &u = GetStr(args[0]);
			const auto &v = GetI64(args[1]);
			std::vector<std::int64_t> out(n);
			if (u.empty()) return out;

			if (AllSame(u)) {
				const std::string unit = u[0];
				for (std::size_t i = 0; i < n; ++i) out[i] = TruncSecondsBy(unit, v[i]);
			} else {
				for (std::size_t i = 0; i < n; ++i) out[i] = TruncSecondsBy(u[i], v[i]);
			}
			return out;
		}

		std::string ConvertBackslashToEcma(const std::string &s) {
			std::string out;
			out.reserve(s.size());
			for (std::size_t i = 0; i < s.size(); ++i) {
				if (s[i] == '\\' && i + 1 < s.size() && s[i + 1] >= '0' && s[i + 1] <= '9') {
					out.push_back('$');
					out.push_back(s[i + 1]);
					++i;
				} else if (s[i] == '$') {
					out.push_back('$');
					out.push_back('$');
				} else {
					out.push_back(s[i]);
				}
			}
			return out;
		}

		EvalCol FnRegexpReplace(const std::vector<EvalCol> &args, std::size_t n) {
			const auto &s = GetStr(args[0]);
			const auto &p = GetStr(args[1]);
			const auto &r = GetStr(args[2]);
			std::vector<std::string> out(n);
			if (p.empty()) return out;

			if (AllSame(p) && AllSame(r)) {
				std::regex re(p[0]);
				std::string ecma_repl = ConvertBackslashToEcma(r[0]);
				for (std::size_t i = 0; i < n; ++i) out[i] = std::regex_replace(s[i], re, ecma_repl);
			} else {
				for (std::size_t i = 0; i < n; ++i) {
					std::regex re(p[i]);
					std::string ecma_repl = ConvertBackslashToEcma(r[i]);
					out[i] = std::regex_replace(s[i], re, ecma_repl);
				}
			}
			return out;
		}

		class FuncCallExpr final : public Expr {
		public:
			FuncCallExpr(const ScalarFn *fn, std::vector<ExprPtr> args)
				: fn_(fn), args_(std::move(args)) {
			}

			EvalType result_type() const override { return fn_->result_type; }

			EvalCol eval(const EvalContext &ctx) const override {
				std::vector<EvalCol> vals;
				vals.reserve(args_.size());
				for (const auto &a: args_) vals.push_back(a->eval(ctx));
				return fn_->impl(vals, ctx.rows());
			}

		private:
			const ScalarFn *fn_;
			std::vector<ExprPtr> args_;
		};

		std::string LowerCopy(const std::string &s) {
			std::string out(s);
			for (auto &c: out) {
				const auto u = static_cast<unsigned char>(c);
				if (u >= 'A' && u <= 'Z') c = static_cast<char>(u + 32);
			}
			return out;
		}
	}

	FuncRegistry::FuncRegistry() {
		auto add = [&](ScalarFn fn) {
			std::string key = LowerCopy(fn.name);
			overloads_[key].push_back(std::move(fn));
		};

		add({"length", {EvalType::Str}, EvalType::I64, FnLength});
		add({"like", {EvalType::Str, EvalType::Str}, EvalType::Bool, FnLike});
		add({"extract", {EvalType::Str, EvalType::DateTime}, EvalType::I64, FnExtractDT});
		add({"extract", {EvalType::Str, EvalType::Date}, EvalType::I64, FnExtractDT});
		add({"date_trunc", {EvalType::Str, EvalType::DateTime}, EvalType::DateTime, FnDateTrunc});
		add({"regexp_replace", {EvalType::Str, EvalType::Str, EvalType::Str}, EvalType::Str, FnRegexpReplace});
	}

	FuncRegistry &FuncRegistry::Instance() {
		static FuncRegistry r;
		return r;
	}

	const ScalarFn *FuncRegistry::Lookup(const std::string &name,
	                                     const std::vector<EvalType> &args) const {
		auto it = overloads_.find(LowerCopy(name));
		if (it == overloads_.end()) return nullptr;
		for (const auto &fn: it->second) {
			if (MatchTypes(fn.arg_types, args)) return &fn;
		}
		return nullptr;
	}

	void FuncRegistry::Add(ScalarFn fn) {
		overloads_[LowerCopy(fn.name)].push_back(std::move(fn));
	}

	ExprPtr MakeFuncCall(const std::string &name, std::vector<ExprPtr> args) {
		std::vector<EvalType> arg_types;
		for (const auto &a: args) arg_types.push_back(a->result_type());
		const ScalarFn *fn = FuncRegistry::Instance().Lookup(name, arg_types);
		if (!fn) throw std::runtime_error("unknown function: " + name);
		return std::make_unique<FuncCallExpr>(fn, std::move(args));
	}
}
