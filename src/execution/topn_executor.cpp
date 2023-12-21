#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  heap_for_topn_.SetMaxIndex(plan_->GetN());
  heap_for_topn_.SetSchema(&plan_->OutputSchema());
  heap_for_topn_.SetOrderBys(plan_->GetOrderBy());
  Tuple tmp_tuple;
  RID rid;
  while (child_executor_.get()->Next(&tmp_tuple, &rid)) {
    // TODO: If the executor can return the numbers, we can resize to the number.
    heap_for_topn_.Insert(tmp_tuple);
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (heap_for_topn_.Size() > 0) {
    *tuple = heap_for_topn_.Top();
    heap_for_topn_.Pop();
    return true;
  } else {
    return false;
  }
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_for_topn_.Size(); };

}  // namespace bustub
