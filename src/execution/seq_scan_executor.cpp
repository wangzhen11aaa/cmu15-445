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
  Init();
}  // namespace bustub

void SeqScanExecutor::Init() {}
/** RID = {page_id, slot_num} */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_.IsEnd())
    return false;
  else {
    TupleMeta tuple_meta;
    do {
      auto [tuple_meta, tuple_] = table_iterator_.GetTuple();
      *tuple = tuple_;
      *rid = table_iterator_.GetRID();
      ++table_iterator_;
    } while (tuple_meta.is_deleted_);
    return true;
  }
}

}  // namespace bustub
