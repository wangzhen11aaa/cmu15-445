//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_{left_executor_->GetOutputSchema()},
      right_schema_{right_executor_->GetOutputSchema()},
      join_output_schema_{plan_->OutputSchema()} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  join_type_ = plan_->GetJoinType();
  std::unordered_map<std::string, int> map_for_left, map_for_right;
  unsigned int i = 0;
  for (const auto &column : left_schema_.GetColumns()) {
    map_for_left[column.GetName()] = i++;
  }
  i = 0;
  for (const auto &column : right_schema_.GetColumns()) {
    map_for_right[column.GetName()] = i++;
  }
  for (const auto &column : join_output_schema_.GetColumns()) {
    if (map_for_left.count(column.GetName())) {
      maps_.push_back({0, map_for_left[column.GetName()]});
    } else {
      BUSTUB_ASSERT(map_for_right.count(column.GetName()) > 0, "column not find ");
      maps_.push_back({1, map_for_right[column.GetName()]});
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple{}, right_tuple{};
  if (join_type_ == JoinType::LEFT) {
    if (left_executor_->Next(&left_tuple, rid)) {
      Value join_value = ValueFactory::GetBooleanValue(false);
      right_executor_->Init();
      while (right_executor_->Next(&right_tuple, rid)) {
        join_value = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema_, &right_tuple, right_schema_);
        if (join_value.GetAs<bool>() == false) {
          continue;
        } else {
          break;
        }
      }
      if (join_value.GetAs<bool>() == false) {
        // Assemble the output tuple.
        std::vector<Value> out_values;
        for (const auto &[pos, column_index] : maps_) {
          if (pos == 0) {
            out_values.push_back(left_tuple.GetValue(&left_schema_, column_index));
          } else {
            out_values.push_back(
                ValueFactory::GetZeroValueByType(right_tuple.GetValue(&right_schema_, column_index).GetTypeId()));
          }
        }
        *tuple = Tuple(out_values, &join_output_schema_);
        return true;
      } else {
        std::vector<Value> out_values;
        for (const auto &[pos, column_index] : maps_) {
          if (pos == 0) {
            out_values.push_back(left_tuple.GetValue(&left_schema_, column_index));
          } else {
            out_values.push_back(right_tuple.GetValue(&right_schema_, column_index));
          }
        }
        *tuple = Tuple(out_values, &join_output_schema_);
        return true;
      }
    } else {
      return false;
    }
  } else if (join_type_ == JoinType::INNER) {
    Value join_value = ValueFactory::GetBooleanValue(false);
    bool left_has_value = false;
    do {
      left_has_value = left_executor_->Next(&left_tuple, rid);
      right_executor_->Init();
      while (right_executor_->Next(&right_tuple, rid)) {
        join_value = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema_, &right_tuple, right_schema_);
        if (join_value.GetAs<bool>() == false) {
          continue;
        } else {
          break;
        }
      }
    } while (join_value.GetAs<bool>() == false && left_has_value);
    if (!left_has_value) {
      return false;
    }
    std::vector<Value> out_values;
    for (const auto &[pos, column_index] : maps_) {
      if (pos == 0) {
        out_values.push_back(left_tuple.GetValue(&left_schema_, column_index));
      } else {
        out_values.push_back(right_tuple.GetValue(&right_schema_, column_index));
      }
    }
    *tuple = Tuple(out_values, &join_output_schema_);
    return true;
  } else {
    BUSTUB_ASSERT(!(join_type_ == JoinType::LEFT || join_type_ == JoinType::INNER), "Not supported now");
    return false;
  }
  return false;
}
}  // namespace bustub
