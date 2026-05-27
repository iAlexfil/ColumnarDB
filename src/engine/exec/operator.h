#pragma once

#include <optional>

#include "exec_batch.h"
#include "schema.h"

namespace exec {
	class Operator {
	public:
		virtual ~Operator() = default;

		virtual const Schema &OutputSchema() const = 0;

		virtual std::optional<ExecBatch> Next() = 0;
	};
}
