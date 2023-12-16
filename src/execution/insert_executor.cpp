//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Init();
}

void InsertExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(catalog, "nullptr");

  table_info_ = catalog->GetTable(plan_->GetTableOid());
  auto indexes_ = catalog->GetTableIndexes(table_info_->name_);

  BUSTUB_ASSERT(table_info_->oid_ == plan_->GetTableOid(), "oid not equal");
  BUSTUB_ASSERT(table_info_->table_ != nullptr, " table heap is nullptr.");
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  uint32_t cnt = 0;
  TupleMeta meta{0, false};
  if (indexes_.size() == 0) {
    while (child_executor_->Next(tuple, rid)) {
      cnt++;
      table_info_->table_->InsertTuple(meta, *tuple);
    }
  } else {
    auto index_info = indexes_[sizeof(indexes_.size() - 1)];
    while (child_executor_->Next(tuple, rid)) {
      cnt++;
      table_info_->table_->InsertTuple(meta, *tuple);
      index_info->index_->InsertEntry(*tuple, *rid, nullptr);
    }
  }
  if (cnt == 0) {
    return false;
  }
  tuple->SetRid({0, cnt});
  return true;
}

}  // namespace bustub
