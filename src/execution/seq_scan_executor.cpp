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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
    bool filter_passed{true};
    TransactionManager *tm = exec_ctx_->GetTransactionManager();
    Transaction *txn = exec_ctx_->GetTransaction();
    do {
      filter_passed = true;
      *rid = table_iterator_.GetRID();
      auto [tuple_meta_bind, tuple_bind] = table_iterator_.GetTuple();
      tuple_meta = std::move(tuple_meta_bind);

      auto read_ts = txn->GetReadTs();
      // Uncommitted tuple used by some transaction.
      if (tuple_meta.ts_ & TXN_START_ID && ((tuple_meta.ts_ ^ TXN_START_ID) == txn->GetTransactionId())) {
        *tuple = tuple_bind;
        if (filter != nullptr) {
          filter_passed = filter->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
        }
      } else {
        // Committed tuple, most recent version is deleted, do not return this tuple to parent executor.
        if (read_ts > tuple_meta.ts_) {
          if (tuple_meta.is_deleted_) {
            filter_passed = false;
            continue;
          } else {
            *tuple = tuple_bind;
          }
        } else {
          // Fetch the first undolog_Link.
          std::optional<UndoLink> undo_link_opt = tm->GetUndoLink(table_iterator_.GetRID());
          if (!undo_link_opt.has_value()) {
            filter_passed = false;
            continue;
          }
          auto undo_link = undo_link_opt.value();
          std::vector<UndoLog> undo_logs{};
          // Loop until read_rs >= undo_link.prev_txn_
          while (undo_link.IsValid()) {
            // Get the undo_log in transaction_j's undo_log vector.
            auto undo_log = tm->GetUndoLog(undo_link);
            if (undo_log.ts_ <= read_ts) {
              undo_logs.push_back(undo_log);
              break;
            }
            undo_logs.push_back(undo_log);
            undo_link = undo_log.prev_version_;
          }
          // No undo_logs exist or no tuple's ts < read_ts
          if (undo_logs.empty() || undo_logs.back().ts_ > read_ts) {
            filter_passed = false;
            continue;
          }
          auto tuple_opt = ReconstructTuple(&plan_->OutputSchema(), tuple_bind, tuple_meta_bind, undo_logs);

          if (tuple_opt.has_value()) {
            *tuple = tuple_opt.value();
          } else {
            filter_passed = false;
            continue;
          }
        }
        if (filter != nullptr) {
          filter_passed = filter->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
        }
      }
    } while ((!((++table_iterator_).IsEnd()) && filter_passed == false));
    if (filter_passed == false) {
      return false;
    }
    return true;
  }
}
}  // namespace bustub
