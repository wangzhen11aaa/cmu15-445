#include <memory>
#include <vector>
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);

      auto &&table_name_in_filter = seq_scan_plan.table_name_;
      std::vector<IndexInfo *> index_infos = catalog_.GetTableIndexes(table_name_in_filter);

      if (filter_plan.GetPredicate().get()->children_.size() != 0 && index_infos.size()) {
        auto &&column_name_in_filter = filter_plan.output_schema_->GetColumns()[0].GetName();
        unsigned int i;
        for (i = 0; i < index_infos.size(); ++i) {
          auto &&column_name_in_index = index_infos[i]->key_schema_.GetColumns()[0].GetName();
          if (column_name_in_filter == table_name_in_filter + '.' + column_name_in_index) {
            break;
          }
        }
        if (i == index_infos.size()) {
          return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                   seq_scan_plan.table_name_, filter_plan.GetPredicate());
        }
        return std::make_shared<IndexScanPlanNode>(
            filter_plan.output_schema_, seq_scan_plan.table_oid_, index_infos[i]->index_oid_,
            filter_plan.GetPredicate(),
            dynamic_cast<ConstantValueExpression *>(filter_plan.predicate_->children_[1].get()));
      } else {
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }
  return optimized_plan;
}  // namespace bustub

}  // namespace bustub
