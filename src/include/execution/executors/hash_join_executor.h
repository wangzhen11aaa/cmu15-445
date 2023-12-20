//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** Compare structure*/
  struct ValueCompare {
    bool operator()(const Value &v1, const Value &v2) const { return v1.CompareLessThan(v2) == CmpBool::CmpTrue; }
  };
  /** The hash table is just a map from aggregate keys to aggregate values */
  typedef std::map<Value, Tuple, ValueCompare> HashMapType;

  /** HashMap for keys of right table*/
  std::unordered_map<std::string, HashMapType> map_;

  /* Left executor*/
  std::unique_ptr<AbstractExecutor> left_child_;
  /** Right executor*/
  std::unique_ptr<AbstractExecutor> right_child_;

  /* Hash initiallized flag*/
  bool hash_table_initialized{false};

  /* Join_type*/
  JoinType join_type_;

  /** Left tuple schema*/
  Schema left_schema_;
  /** Right tuple schema*/
  Schema right_schema_;
  /** Join output tuple schema*/
  Schema join_output_schema_;

  /** Map from output schema index to input source schema
   * items in pair: 0:left_executor, x: column_index in left tuple, 1: right_executor, y: column_index in right tuple.
   */
  std::vector<std::pair<int, int>> pos_map_to_idx_;
};

}  // namespace bustub
