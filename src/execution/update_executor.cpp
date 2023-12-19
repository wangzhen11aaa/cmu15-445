//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(catalog, "nullptr");

  table_info_ = catalog->GetTable(plan_->GetTableOid());

  BUSTUB_ASSERT(table_info_->oid_ == plan_->GetTableOid(), "oid not equal");
  BUSTUB_ASSERT(table_info_->table_ != nullptr, " table heap is nullptr.");

  indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  TupleMeta positive_meta{0, false}, negative_meta{0, true};
  if (indexes_.size() == 0) {
    if (child_executor_->Next(tuple, rid)) {
      table_info_->table_->UpdateTupleMeta(negative_meta, *rid);
      Schema input_schema = child_executor_->GetOutputSchema();
      Schema update_schema = plan_->OutputSchema();

      std::vector<Value> tuple_values(input_schema.GetColumnCount(), Value{});
      for (size_t i = 0; i < input_schema.GetColumnCount(); i++) {
        tuple_values[i] = plan_->target_expressions_[i]->Evaluate(tuple, input_schema);
      }

      Tuple tuple_to_insert{tuple_values, &input_schema};
      table_info_->table_->InsertTuple(positive_meta, tuple_to_insert);
      return true;
    }
    return false;
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
      // "Mark tuples as deleted, mappings: 1(tuple): 1(rip)"
      for (auto &rid_to_del : result) {
        table_info_->table_->UpdateTupleMeta(negative_meta, rid_to_del);
        index_info->index_->DeleteEntry(index_tuple, rid_to_del, nullptr);
      }
      std::vector<Value> tuple_values;
      auto fields_num = plan_->target_expressions_.size();
      for (size_t i = 0; i < fields_num; i++) {
        tuple_values.push_back(plan_->target_expressions_[i]->Evaluate(tuple, plan_->OutputSchema()));
      }
      Tuple tuple_to_insert{tuple_values, &(plan_->OutputSchema())};
      std::optional<RID> rid_opt = table_info_->table_->InsertTuple(positive_meta, tuple_to_insert);
      if (rid_opt.has_value()) {
        index_info->index_->InsertEntry(tuple_to_insert, rid_opt.value(), nullptr);
        *rid = rid_opt.value();
        return true;
      }
      return false;
    }
    return false;
  }
}

}  // namespace bustub
