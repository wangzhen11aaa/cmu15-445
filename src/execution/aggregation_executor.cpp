//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()},
      aht_iterator_{aht_.End()} {}

void AggregationExecutor::Init() { aht_.GenerateInitialAggregateValue(); }

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    auto aggregate_keys = MakeAggregateKey(tuple);
    auto aggregate_values = MakeAggregateValue(tuple);
    aht_.InsertCombine(aggregate_keys, aggregate_values);
  }
  if (!begin_iterate_) {
    begin_iterate_ = true;
    aht_iterator_ = {aht_.Begin()};
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  } else {
    auto aggregate_schema = GetOutputSchema();
    std::vector<Value> output_values{};

    for (const auto &out_column : aggregate_schema.GetColumns()) {
      auto out_column_name = out_column.GetName();
      if (name_to_group_id_.count(out_column_name)) {
        output_values.push_back(aht_iterator_.Key().group_bys_[name_to_group_id_[out_column_name]]);
      }
    }
    // Compare the outschema of the aggregationExecutor and the child's out schema.
    for (unsigned int i = 0; i < aht_iterator_.Val().aggregates_.size(); i++) {
      output_values.push_back(aht_iterator_.Val().aggregates_[i]);
    }

    *tuple = Tuple(output_values, &aggregate_schema);
    ++aht_iterator_;
    return true;
  }
}  // namespace bustub

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
