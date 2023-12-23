//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> agg_exprs,
                             const std::vector<AggregationType> agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (AggregationType agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      Value res{ValueFactory::GetIntegerValue(0)};
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
          res = ValueFactory::GetIntegerValue(result->aggregates_[i].GetAs<uint32_t>() + input.aggregates_.size());
          result->aggregates_[i] = result->aggregates_[i].Add(res);
          break;
        case AggregationType::CountAggregate:
          res = ValueFactory::GetIntegerValue(std::count_if(std::begin(input.aggregates_), std::end(input.aggregates_),
                                                            [](const Value &v) { return !v.IsNull(); }));
          if (result->aggregates_[i].IsNull()) {
            Swap(result->aggregates_[i], res);
          } else {
            result->aggregates_[i] = result->aggregates_[i].Add(res);
          }
          break;
        case AggregationType::SumAggregate:
          std::for_each(std::begin(input.aggregates_), std::end(input.aggregates_), [&](const Value &v0) {
            if (!v0.IsNull()) {
              res = res.Add(v0);
            }
          });
          if (result->aggregates_[i].IsNull()) {
            Swap(result->aggregates_[i], res);
          } else {
            result->aggregates_[i] = result->aggregates_[i].Add(res);
          }
          break;
        case AggregationType::MinAggregate:
          res = *(std::min_element(std::begin(input.aggregates_), std::end(input.aggregates_),
                                   [](const Value &first, const Value &second) {
                                     return first.CompareLessThan(second) == CmpBool::CmpTrue;
                                   }));
          if (result->aggregates_[i].IsNull()) {
            Swap(result->aggregates_[i], res);
          } else {
            if (result->aggregates_[i].CompareGreaterThan(res) == CmpBool::CmpTrue) {
              Swap(result->aggregates_[i], res);
            }
          }
          break;
        case AggregationType::MaxAggregate:
          res = *(std::max_element(std::begin(input.aggregates_), std::end(input.aggregates_),
                                   [](const Value &first, const Value &second) {
                                     return first.CompareGreaterThan(second) == CmpBool::CmpTrue;
                                   }));
          if (result->aggregates_[i].IsNull()) {
            Swap(result->aggregates_[i], res);
          } else {
            if (result->aggregates_[i].CompareLessThan(res) == CmpBool::CmpTrue) {
              Swap(result->aggregates_[i], res);
            }
          }
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> agg_exprs_;
  /** The types of aggregations that we have */
  const std::vector<AggregationType> agg_types_;
};  // namespace bustub

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    unsigned int idx = 0;
    if (plan_->GetGroupBys().size() == 0) {
      keys.emplace_back(Value{ValueFactory::GetIntegerValue(1)});
    } else {
      for (const auto &expr : plan_->GetGroupBys()) {
        ColumnValueExpression *column_expression = dynamic_cast<ColumnValueExpression *>(expr.get());
        if (column_expression) {
          name_to_group_id_[child_executor_->GetOutputSchema().GetColumn(column_expression->GetColIdx()).GetName()] =
              idx++;
        }
        keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
      }
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;

  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Simple aggregation hash table */
  SimpleAggregationHashTable aht_;

  /** Simple aggregation hash table iterator */
  SimpleAggregationHashTable::Iterator aht_iterator_;

  /** Flag to mark that the iteration has begin*/
  bool begin_iterate_{false};

  /**Name maps to group_bys id*/
  std::unordered_map<std::string, int> name_to_group_id_;
};
}  // namespace bustub
