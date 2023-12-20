//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      left_schema_{left_child_->GetOutputSchema()},
      right_schema_{right_child_->GetOutputSchema()},
      join_output_schema_{plan_->OutputSchema()} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  BUSTUB_ASSERT(plan_->left_key_expressions_.size() == plan_->right_key_expressions_.size(),
                "Not join parts not equal");
  join_type_ = plan_->GetJoinType();
  // Initialize the hash_map for the join column
  ColumnValueExpression *column_value_expr;
  for (auto &expr_to_join : plan_->right_key_expressions_) {
    column_value_expr = dynamic_cast<ColumnValueExpression *>(expr_to_join.get());
    if (column_value_expr) {
      map_[right_child_->GetOutputSchema().GetColumns()[column_value_expr->GetColIdx()].GetName()] = HashMapType{};
    }
  }

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
      pos_map_to_idx_.push_back({0, map_for_left[column.GetName()]});
    } else {
      BUSTUB_ASSERT(map_for_right.count(column.GetName()) > 0, "column not find ");
      pos_map_to_idx_.push_back({1, map_for_right[column.GetName()]});
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  /*Construct hashtable from right_executor*/
  if (!hash_table_initialized) {
    Tuple right_tuple;
    ColumnValueExpression *column_value_expr;

    while (right_child_->Next(&right_tuple, rid) == true) {
      for (auto expr : plan_->right_key_expressions_) {
        column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get());
        auto value = column_value_expr->Evaluate(&right_tuple, right_schema_);
        map_[right_schema_.GetColumns()[column_value_expr->GetColIdx()].GetName()][value] = right_tuple;
      }
    }
    hash_table_initialized = true;
  }

  Tuple left_tuple{}, right_tuple_to_match{};
  unsigned int i = 0;
  Value left_value;
  bool match{true};
  std::string column_name_to_match;
  bool left_has_value = true;
  do {
    left_has_value = left_child_->Next(&left_tuple, rid) == true;
    if (!left_has_value) return false;

    for (; i < plan_->left_key_expressions_.size(); i++) {
      auto column_value_expr_left = dynamic_cast<ColumnValueExpression *>(plan_->left_key_expressions_[i].get());
      left_value = column_value_expr_left->Evaluate(&left_tuple, left_schema_);

      auto column_value_expr_right = dynamic_cast<ColumnValueExpression *>(plan_->right_key_expressions_[i].get());
      column_name_to_match =
          right_child_->GetOutputSchema().GetColumns()[column_value_expr_right->GetColIdx()].GetName();

      if (!map_[column_name_to_match].count(left_value)) {
        match = false;
        break;
      } else {
        right_tuple_to_match = map_[column_name_to_match][left_value];
      }
    }

    if (match) {
      std::vector<Value> out_values;
      for (const auto &[pos, column_index] : pos_map_to_idx_) {
        if (pos == 0) {
          out_values.push_back(left_tuple.GetValue(&left_schema_, column_index));
        } else {
          out_values.push_back(right_tuple_to_match.GetValue(&right_schema_, column_index));
        }
      }
      *tuple = Tuple(out_values, &join_output_schema_);
      return true;
    } else {
      if (join_type_ == JoinType::LEFT) {
        std::vector<Value> out_values;
        for (const auto &[pos, column_index] : pos_map_to_idx_) {
          if (pos == 0) {
            out_values.push_back(left_tuple.GetValue(&left_schema_, column_index));
          } else {
            out_values.push_back(ValueFactory::GetZeroValueByType(TypeId::BOOLEAN));
          }
        }
        *tuple = Tuple(out_values, &join_output_schema_);
        return true;
      }
    }
  } while (join_type_ == JoinType::INNER && left_has_value);
  return false;
}
}  // namespace bustub
