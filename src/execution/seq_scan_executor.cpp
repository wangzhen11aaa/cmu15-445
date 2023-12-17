//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_iterator_{exec_ctx->GetCatalog()->GetTable(plan->table_name_)->table_->MakeIterator()} {
}  // namespace bustub

void SeqScanExecutor::Init() {}
/** RID = {page_id, slot_num} */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_.IsEnd())
    return false;
  else {
    TupleMeta tuple_meta;
    auto filter = plan_->filter_predicate_;
    bool filter_result{true};
    do {
      auto [tuple_meta_bind, tuple_bind] = table_iterator_.GetTuple();
      tuple_meta = std::move(tuple_meta_bind);
      *tuple = tuple_bind;
      *rid = table_iterator_.GetRID();
      ++table_iterator_;
      if (filter != nullptr) {
        filter_result = filter->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
      }
    } while ((filter_result == false || tuple_meta.is_deleted_) && !table_iterator_.IsEnd());
    if (tuple_meta.is_deleted_ || filter_result == false) {
      return false;
    }
    return true;
  }
}

}  // namespace bustub
