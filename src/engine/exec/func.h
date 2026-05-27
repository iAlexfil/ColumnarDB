#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "expr.h"

namespace exec {
	struct ScalarFn {
		std::string name;
		std::vector<EvalType> arg_types;
		EvalType result_type;
		std::function<EvalCol(const std::vector<EvalCol> &, std::size_t rows)> impl;
	};

	class FuncRegistry {
	public:
		static FuncRegistry &Instance();

		const ScalarFn *Lookup(const std::string &name, const std::vector<EvalType> &args) const;

		void Add(ScalarFn fn);

	private:
		FuncRegistry();

		std::unordered_map<std::string, std::vector<ScalarFn> > overloads_;
	};

	ExprPtr MakeFuncCall(const std::string &name, std::vector<ExprPtr> args);
}
