//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }
  /** @return vector of Values*/
 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Store all the Tuples*/
  std::vector<Tuple> tuples_;

  /** Iterator for all tuples*/
  std::vector<Tuple>::iterator iter_;

  /** class used to execute the window_function*/
  class WindowAggregate {
   public:
    using Iterator_ = std::vector<Tuple>::iterator;
    WindowAggregate(const std::vector<AbstractExpressionRef> &columns, AbstractExpressionRef &expr, uint32_t column_idx,
                    WindowFunctionType win_function_type, const Schema &schema)
        : columns_(columns),
          expr_(expr),
          function_column_idx_(column_idx),
          window_function_type_(win_function_type),
          schema_(&schema) {}

    // Aggregate the tuples [it0, it1);
    void Compute(Iterator_ it0, Iterator_ it1) {
      AggregationType agg_type;

      switch (window_function_type_) {
        case WindowFunctionType::CountAggregate: {
          agg_type = AggregationType::CountAggregate;
          break;
          case WindowFunctionType::CountStarAggregate:
            agg_type = AggregationType::CountStarAggregate;
            break;
          case WindowFunctionType::SumAggregate:
            agg_type = AggregationType::SumAggregate;
            break;
          case WindowFunctionType::MinAggregate:
            agg_type = AggregationType::MinAggregate;
            break;
          case WindowFunctionType::MaxAggregate:
            agg_type = AggregationType::MaxAggregate;
            break;
          case WindowFunctionType::Rank:
            simple_aggregate_flag_ = false;
            break;
        }
      }
      // 获取聚合结果在tuple中的下标位置。
      if (simple_aggregate_flag_) {
        SimpleAggregationHashTable aht{std::vector<AbstractExpressionRef>{expr_},
                                       std::vector<AggregationType>{agg_type}};

        aht.GenerateInitialAggregateValue();
        AggregateValue agg_value;
        AggregateKey keys;
        agg_value.aggregates_.resize(1);
        keys.group_bys_.resize(1);
        Iterator_ cur_it = it0;
        // key in [it0, it1) is the same, take the first value as key.
        keys.group_bys_[0] = dynamic_cast<ColumnValueExpression *>(expr_.get())->Evaluate(&(*cur_it), *schema_);
        for (; cur_it != it1; ++cur_it) {
          agg_value.aggregates_[0] = dynamic_cast<ColumnValueExpression *>(expr_.get())->Evaluate(&(*cur_it), *schema_);
          aht.InsertCombine(keys, agg_value);
        }
        cur_it = it0;

        std::vector<Value> values_;
        values_.resize(schema_->GetColumns().size());
        for (; cur_it != it1; ++cur_it) {
          for (uint32_t i = 0; i < schema_->GetColumns().size(); i++) {
            if (i != function_column_idx_) {
              values_[i] = columns_[i].get()->Evaluate(&(*cur_it), *schema_);
            } else {
              values_[i] = aht.Begin().Val().aggregates_[0];
            }
          }
          // 更新Iterator指向的tuple
          *cur_it = Tuple{values_, schema_};
        }
      } else {
        std::vector<Value> values_;
        Iterator_ cur_it = it0;
        values_.resize(schema_->GetColumns().size());
        auto rank_i = 1;
        for (; cur_it != it1; ++cur_it) {
          for (uint32_t i = 0; i < schema_->GetColumns().size(); i++) {
            if (i != function_column_idx_) {
              values_[i] = columns_[i].get()->Evaluate(&(*cur_it), *schema_);
            } else {
              values_[i] = Value{ValueFactory::GetIntegerValue(rank_i++)};
            }
          }
          // 更新Iterator指向的tuple
          *cur_it = Tuple{values_, schema_};
        }
      }
    }

   private:
    const std::vector<AbstractExpressionRef> &columns_;
    AbstractExpressionRef &expr_;
    uint32_t function_column_idx_;
    WindowFunctionType window_function_type_;
    bool simple_aggregate_flag_{true};
    const Schema *schema_;
  };
};
}  // namespace bustub
