//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(catalog, "nullptr");

  table_info_ = catalog->GetTable(plan_->GetTableOid());

  BUSTUB_ASSERT(table_info_ != nullptr, " not found!");
  BUSTUB_ASSERT(table_info_->oid_ == plan_->GetTableOid(), "table oid not equal");
  BUSTUB_ASSERT(table_info_->table_ != nullptr, " table heap is nullptr.");

  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  TupleMeta negative_meta{0, true};
  if (indexes_.size() == 0) {
    if (child_executor_->Next(tuple, rid)) {
      table_info_->table_->UpdateTupleMeta(negative_meta, *rid);
      return true;
    } else {
      return false;
    }
  } else {
    auto index_info = indexes_[0];
    std::vector<RID> result;
    if (child_executor_->Next(tuple, rid)) {
      auto index_column_schema = index_info->index_->GetKeySchema();
      auto index_column_name = index_info->index_->GetKeySchema()->GetColumn(0).GetName();
      auto index_column_index = index_info->index_->GetKeySchema()->GetColIdx(index_column_name);
      auto index_column_value = tuple->GetValue(index_column_schema, index_column_index);

      Tuple index_tuple{std::vector<Value>{index_column_value}, index_column_schema};

      index_info->index_->ScanKey(index_tuple, &result, nullptr);
      if (result.size() == 0) {
        return false;
      }

      BUSTUB_ASSERT(result.size() == 1, "tuple not unique in one page");
      for (auto &rid_to_del : result) {
        table_info_->table_->UpdateTupleMeta(negative_meta, rid_to_del);
        index_info->index_->DeleteEntry(index_tuple, rid_to_del, nullptr);
      }
      return true;
    }
    return false;
  }
}

}  // namespace bustub
