//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Store all the tuples*/
  std::vector<Tuple> tuples_;

  /** Iteartor the vector*/
  std::vector<Tuple>::iterator iter_;

  /** Child executor for pull data*/
  std::unique_ptr<AbstractExecutor> child_executor_;
  /**Comparator for Value*/
  class ValueComparator {
   public:
    void SetOrderBy(std::pair<OrderByType, AbstractExpressionRef> *order_by_expr) { order_by_pair_ = order_by_expr; }

    bool operator()(const Tuple &v1, const Tuple &v2) const {
      auto [order_type, column_value_expression] = *order_by_pair_;
      if (order_type == OrderByType::ASC) {
        auto compare_result = column_value_expression.get()
                                  ->Evaluate(&v1, *schema_)
                                  .CompareLessThanEquals(column_value_expression.get()->Evaluate(&v2, *schema_));
        if (compare_result == CmpBool::CmpTrue) {
          return true;
        } else {
          return false;
        }
      } else {
        auto compare_result = column_value_expression.get()
                                  ->Evaluate(&v1, *schema_)
                                  .CompareGreaterThanEquals(column_value_expression.get()->Evaluate(&v2, *schema_));
        if (compare_result == CmpBool::CmpTrue) {
          return true;
        } else {
          return false;
        }
      }
    }

    std::pair<OrderByType, AbstractExpressionRef> *order_by_pair_;
    const Schema *schema_;
  };
};  // namespace bustub
}  // namespace bustub
