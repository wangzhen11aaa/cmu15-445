#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tmp_tuple;
  RID rid;
  while (child_executor_.get()->Next(&tmp_tuple, &rid)) {
    // TODO: If the executor can return the numbers, we can resize to the number.
    tuples_.push_back(tmp_tuple);
  }

  std::pair<OrderByType, AbstractExpressionRef> prev_order{};
  ValueComparator curr_comparator{};
  curr_comparator.schema_ = &plan_->OutputSchema();
  curr_comparator.SetOrderBy(&plan_->order_bys_);
  // E.g:
  // order by col1 desc, col2 asc
  std::sort(tuples_.begin(), tuples_.end(), curr_comparator);

  iter_ = tuples_.begin();
}  // namespace bustub

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != tuples_.end()) {
    *tuple = *iter_;
    iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
