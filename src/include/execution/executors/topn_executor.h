//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class HeapForTopN {
 public:
  HeapForTopN() = default;
  void SetMaxIndex(int size) { max_index_ = size; }
  void SetSchema(const Schema *schema) { value_comparator_.schema_ = schema; }
  void SetOrderBys(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_ptr) {
    order_bys_ptr_ = &order_by_ptr;
    tuples_.push_back(Tuple{});
    curr_index_ = 0;
  }

  unsigned int Size() { return curr_index_; }
  void Insert(Tuple tuple) {
    if (curr_index_ < max_index_) {
      tuples_.push_back(tuple);
      curr_index_++;
      HeapifyFromLeaf();
    } else {
      auto tuple_root = tuples_[1];
      value_comparator_.SetOrderBy(order_bys_ptr_);
      // Verify tuples with each order_exp.
      if (value_comparator_(tuple_root, tuple) == true) {
        tuples_[1] = tuple;
        HeapifyFromRoot(1);
      }
    }
  }

  Tuple Top() { return tuples_[1]; }
  void Pop() {
    tuples_[1] = tuples_[curr_index_];
    curr_index_--;
    HeapifyFromRoot(1);
  }

 private:
  int parent(int i) { return i / 2; }
  int left_child(int i) { return 2 * i; };
  int right_child(int i) { return 2 * i + 1; };

  void HeapifyFromRoot(int i) {
    int left_child_i = left_child(i);
    int right_child_i = right_child(i);
    value_comparator_.SetOrderBy(order_bys_ptr_);
    while (right_child_i <= curr_index_ && right_child_i <= curr_index_) {
      auto tuple_left = tuples_[left_child_i];
      auto tuple_right = tuples_[right_child_i];
      int target_index;
      if (value_comparator_(tuple_left, tuple_right) == false) {
        target_index = left_child_i;
      } else {
        target_index = right_child_i;
      }
      if (value_comparator_(tuples_[target_index], tuples_[i]) == true) {
        std::swap(tuples_[i], tuples_[target_index]);
        i = target_index;
        left_child_i = left_child(i);
        right_child_i = right_child(i);
      } else {
        break;
      }
    }
    if (left_child_i <= curr_index_) {
      if (value_comparator_(tuples_[i], tuples_[left_child_i]) == true) {
        std::swap(tuples_[i], tuples_[left_child_i]);
      }
    }
  }
  // TODO compare function with all expressions can be in one function.
  void HeapifyFromLeaf() {
    int i = curr_index_;
    while (i > 1) {
      auto p_i = parent(i);
      auto tuple_i = tuples_[i];
      auto tuple_p = tuples_[p_i];
      value_comparator_.SetOrderBy(order_bys_ptr_);
      // Verify tuples with each order_exp.
      if (value_comparator_(tuple_i, tuple_p) == true) {
        std::swap(tuple_i, tuple_p);
        i = p_i;
      }
    }
  }

  int max_index_{0};
  /** cur_index_ points to the last tuple in the heap*/
  int curr_index_;
  std::vector<Tuple> tuples_;
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *order_bys_ptr_;
  ValueComparator value_comparator_{};
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  HeapForTopN heap_for_topn_;
};
}  // namespace bustub
