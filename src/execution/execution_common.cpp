#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

void ConstructPartialValues(const Schema *partial_schema, Tuple &tuple, std::vector<Value> &values) {
  for (uint32_t i = 0; i < partial_schema->GetColumns().size(); i++) {
    values.push_back(tuple.GetValue(partial_schema, i));
  }
}
Schema ConstructParitialSchema(const Schema *schema, std::vector<bool> &modified_fileds) {
  std::vector<Column> partial_columns;
  for (uint32_t i = 0; i < schema->GetColumns().size(); i++) {
    if (modified_fileds[i]) {
      partial_columns.push_back(schema->GetColumn(i));
    }
  }
  return Schema{partial_columns};
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  Tuple curr_tuple = base_tuple;
  if (base_meta.is_deleted_) {
    curr_tuple = Tuple{};
  }
  std::vector<Value> values{};
  for (auto undo_log : undo_logs) {
    values.resize(schema->GetColumns().size());
    if (undo_log.is_deleted_) {
      // values stay unchaged.
      curr_tuple = {};
      continue;
    }
    Schema partial_schema = ConstructParitialSchema(schema, undo_log.modified_fields_);

    std::vector<Value> partial_values;
    if (!IsTupleContentEqual(undo_log.tuple_, Tuple{})) {
      ConstructPartialValues(&partial_schema, undo_log.tuple_, partial_values);
    }
    uint32_t j = 0;
    for (uint32_t i = 0; i < schema->GetColumns().size(); i++) {
      if (undo_log.modified_fields_[i]) {
        values[i] = partial_values[j++];
        continue;
      }
      values[i] = curr_tuple.GetValue(schema, i);
    }
    curr_tuple = {values, schema};
  }
  if (IsTupleContentEqual(curr_tuple, Tuple{})) {
    return std::nullopt;
  } else {
    return std::make_optional<Tuple>(curr_tuple);
  }
}  // namespace bustub

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}, table name: {}", info, table_info->name_);

  auto iter = table_heap->MakeIterator();
  RID rid;
  do {
    rid = iter.GetRID();
    auto [tuple_meta_bind, tuple_bind] = iter.GetTuple();
    fmt::println(stderr, "RID={}/{}, ts={}, tuple = {}", rid.GetPageId(), rid.GetSlotNum(), tuple_meta_bind.ts_,
                 tuple_bind.ToString(&table_info->schema_));

    // First undolog
    auto undo_link_opt = txn_mgr->GetUndoLink(rid);
    if (undo_link_opt.has_value()) {
      auto undo_link = undo_link_opt.value();
      while (undo_link.IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(undo_link);
        if (undo_log.is_deleted_) {
          fmt::println(stderr, "txn {}, is_delete={}, ts={}, tuple = {}", undo_link.prev_txn_,
                       undo_log.is_deleted_ ? std::string{"deleted"} : std::string{"not-deleted"}, undo_log.ts_, "");
        } else {
          fmt::println(stderr, "txn {}, is_delete={}, ts={}, tuple = {}", undo_link.prev_txn_,
                       undo_log.is_deleted_ ? std::string{"deleted"} : std::string{"not-deleted"}, undo_log.ts_,
                       undo_log.tuple_.ToString(&table_info->schema_));
        }
        undo_link = undo_log.prev_version_;
      }
    }
  } while (!(++iter).IsEnd());

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}  // namespace bustub

}  // namespace bustub
