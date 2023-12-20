#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  while (child_executor_.get()->Next(&tuple, &rid)) {
    // TODO: If the executor can return the numbers, we can resize to the number.
    tuples_.push_back(tuple);
  }

  ValueComparator comparator{};
  comparator.schema_ = &(plan_->OutputSchema());

  bool first_order_flag = true;
  auto it0 = tuples_.begin(), it1 = tuples_.end();
  std::pair<OrderByType, AbstractExpressionRef> prev_order{};
  ValueComparator prev_comparator{}, curr_comparator{};
  curr_comparator.schema_ = &plan_->OutputSchema();
  for (auto order_exp : plan_->order_bys_) {
    if (first_order_flag) {
      curr_comparator.SetOrderBy(&order_exp);
      std::sort(it0, it1, curr_comparator);
      prev_comparator = curr_comparator;
      first_order_flag = false;
    } else {
      it0 = tuples_.begin();
      it1 = std::upper_bound(it0, tuples_.end(), *it0, prev_comparator);

      curr_comparator.SetOrderBy(&order_exp);

      while (it0 != tuples_.end()) {
        std::sort(it0, it1, curr_comparator);
        it0 = it1;
        it1 = std::upper_bound(it0, tuples_.end(), *it0, prev_comparator);
      }

      prev_comparator = curr_comparator;
    }
  }

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
