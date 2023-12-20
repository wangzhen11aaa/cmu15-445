#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

class Converter {
 public:
  std::vector<AbstractExpressionRef> left_children;
  std::vector<AbstractExpressionRef> right_children;
  bool convert_success{true};

  void convert(AbstractExpressionRef root, bool left) {
    if (!convert_success) return;
    if (root.get()->GetChildren().size() != 0 && root.get()->GetChildAt(0).get() != nullptr &&
        root->GetChildAt(1).get() != nullptr) {
      LogicExpression *v = dynamic_cast<LogicExpression *>(root.get());
      if (v == nullptr) {
        convert_success = false;
      } else {
        if (v->logic_type_ != LogicType::And) {
          convert_success = false;
          return;
        }
      }
      if (convert_success == false) {
        ComparisonExpression *v1 = dynamic_cast<ComparisonExpression *>(root.get());
        if (v1 == nullptr) {
          return;
        }
      }
      convert_success = true;
      convert(root->GetChildAt(0), true);
      convert(root->GetChildAt(1), false);
    } else {
      ColumnValueExpression *col1 = dynamic_cast<ColumnValueExpression *>(root.get());
      if (col1 == nullptr) {
        convert_success = false;
        return;
      }
      if (left) {
        left_children.push_back(root);
      } else {
        right_children.push_back(root);
      }
    }
  }
};

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    auto predicate_expr_ptr = nested_loop_join_plan.Predicate();
    Converter converter;
    converter.convert(predicate_expr_ptr, false);

    if (!converter.convert_success) {
      return optimized_plan;
    } else {
      // return optimized_plan;
      return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_, optimized_plan->GetChildAt(0),
                                                optimized_plan->GetChildAt(1), converter.left_children,
                                                converter.right_children, nested_loop_join_plan.GetJoinType());
    }
  }
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  return optimized_plan;
}

}  // namespace bustub
