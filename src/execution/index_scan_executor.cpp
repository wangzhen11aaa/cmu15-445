//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  Init();
}

void IndexScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(catalog, "nullptr");

  table_info_ = catalog->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info_, "not found!");

  BUSTUB_ASSERT(table_info_->table_ != nullptr, " table heap is nullptr.");

  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_info_->index_ == nullptr || rid->GetSlotNum() == end_rid_.GetSlotNum()) {
    return false;
  }
  std::vector<RID> result;
  Value value_to_search = plan_->pred_key_->Evaluate(tuple, plan_->OutputSchema());

  Schema schema_to_search{std::vector<Column>{plan_->OutputSchema().GetColumns()[0]}};
  Tuple tuple_to_search{std::vector<Value>{value_to_search}, &schema_to_search};

  index_info_->index_->ScanKey(tuple_to_search, &result, nullptr);
  if (result.size() == 0) {
    return false;
  }
  *rid = result[0];
  auto [tuple_meta, tuple_ret] = table_info_->table_->GetTuple(*rid);

  auto filter = plan_->filter_predicate_;
  bool filter_result{false};

  filter_result = filter->Evaluate(&tuple_ret, plan_->OutputSchema()).GetAs<bool>();
  if (tuple_meta.is_deleted_ || !filter_result) {
    return false;
  } else {
    *tuple = std::move(tuple_ret);
    *rid = {INVALID_PAGE_ID, UINT32_MAX};
    return true;
  }
}

}  // namespace bustub
