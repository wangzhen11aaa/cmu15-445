#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    if (optimized_plan->GetChildren().size() == 1 && optimized_plan->GetChildAt(0).get()->GetType() == PlanType::Sort) {
      auto limit_plan_ptr = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
      BUSTUB_ASSERT(limit_plan_ptr != nullptr, "LimitPlanNode nullptr");

      auto limit = limit_plan_ptr->GetLimit();

      auto sort_plan_ptr = dynamic_cast<const SortPlanNode *>(limit_plan_ptr->GetChildPlan().get());

      BUSTUB_ASSERT(sort_plan_ptr != nullptr, "SortPlanNode nullptr");

      return std::make_shared<TopNPlanNode>(limit_plan_ptr->output_schema_, sort_plan_ptr->GetChildPlan(),
                                            sort_plan_ptr->GetOrderBy(), limit);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
