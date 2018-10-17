package husky.sql.calcite.plan.rules;

import husky.sql.calcite.plan.logical.EnumerableToLogicalTableScan;
import husky.sql.calcite.plan.nodes.logical.converter.*;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import org.apache.calcite.rel.rules.*;

public class HuskyRuleSets {

    /**
     * Convert sub-queries before query decorrelation.
     */
    public static RuleSet TABLE_SUBQUERY_RULES = RuleSets.ofList(
            SubQueryRemoveRule.FILTER,
            SubQueryRemoveRule.PROJECT,
            SubQueryRemoveRule.JOIN);

    /**
     * Convert table references before query decorrelation.
     */
    public static RuleSet TABLE_REF_RULES = RuleSets.ofList(
            TableScanRule.INSTANCE,
            EnumerableToLogicalTableScan.INSTANCE);

    public static RuleSet LOGICAL_OPT_RULES = RuleSets.ofList(

            // push a filter into a join
            FilterJoinRule.FILTER_ON_JOIN,
            // push filter into the children of a join
            FilterJoinRule.JOIN,
            // push filter through an aggregation
            FilterAggregateTransposeRule.INSTANCE,
            // push filter through set operation
            FilterSetOpTransposeRule.INSTANCE,
            // push project through set operation
            ProjectSetOpTransposeRule.INSTANCE,

            // aggregation and projection rules
            AggregateProjectMergeRule.INSTANCE,
            AggregateProjectPullUpConstantsRule.INSTANCE,
            // push a projection past a filter or vice versa
            ProjectFilterTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            // push a projection to the children of a join
            // push all expressions to handle the time indicator correctly
            new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
            // merge projections
            ProjectMergeRule.INSTANCE,
            // remove identity project
            ProjectRemoveRule.INSTANCE,
            // reorder sort and projection
            SortProjectTransposeRule.INSTANCE,
            ProjectSortTransposeRule.INSTANCE,

            // join rules
            JoinPushExpressionsRule.INSTANCE,

            // remove union with only a single child
            UnionEliminatorRule.INSTANCE,
            // convert non-all union into all-union + distinct
            UnionToDistinctRule.INSTANCE,

            // remove aggregation if it does not aggregate and input is already distinct
            AggregateRemoveRule.INSTANCE,
            // push aggregate through join
            AggregateJoinTransposeRule.EXTENDED,
            // aggregate union rule
            AggregateUnionAggregateRule.INSTANCE,
            // expand distinct aggregate to normal aggregate with groupby
            AggregateExpandDistinctAggregatesRule.JOIN,

            // reduce aggregate functions like AVG, STDDEV_POP etc.
            AggregateReduceFunctionsRule.INSTANCE,
//            WindowAggregateReduceFunctionsRule.INSTANCE,

            // remove unnecessary sort rule
            SortRemoveRule.INSTANCE,

            // prune empty results rules
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE,
            PruneEmptyRules.UNION_INSTANCE,

            // calc rules
            FilterCalcMergeRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            ProjectToCalcRule.INSTANCE,
            CalcMergeRule.INSTANCE,

            // scan optimization
//            PushProjectIntoTableSourceScanRule.INSTANCE,
//            PushFilterIntoTableSourceScanRule.INSTANCE,

            // unnest rule
//            LogicalUnnestRule.INSTANCE,

//            translate to husky logical rel nodes
//            HuskyLogicalAggregate.CONVERTER,
//            HuskyLogicalWindowAggregate.CONVERTER,
//            HuskyLogicalOverWindow.CONVERTER,
            HuskyLogicalCalcConverter.INSTANCE,
//            HuskyLogicalCorrelate.CONVERTER,
//            HuskyLogicalIntersect.CONVERTER,
//            HuskyLogicalJoin.CONVERTER,
//            HuskyLogicalMinus.CONVERTER,
//            HuskyLogicalSort.CONVERTER,
//            HuskyLogicalUnion.CONVERTER,
//            HuskyLogicalValues.CONVERTER,
//            HuskyLogicalTableSourceScanConverter.INSTANCE,
//            HuskyLogicalTableFunctionScan.CONVERTER,
            HuskyLogicalNativeTableScanConverter.INSTANCE
    );

    /**
     * RuleSet to normalize plans for batch / DataSet execution
     */
    public static RuleSet NORM_RULES = RuleSets.ofList(
            // simplify expressions rules
            ReduceExpressionsRule.FILTER_INSTANCE,
            ReduceExpressionsRule.PROJECT_INSTANCE,
            ReduceExpressionsRule.CALC_INSTANCE,
            ReduceExpressionsRule.JOIN_INSTANCE,
            ProjectToWindowRule.PROJECT

            // Transform grouping sets
//            DecomposeGroupingSetRule.INSTANCE,

            // Transform window to LogicalWindowAggregate
//            DataSetLogicalWindowAggregateRule.INSTANCE,
//            WindowPropertiesRule.INSTANCE,
//            WindowPropertiesHavingRule.INSTANCE
    );

    /**
     * RuleSet to optimize plans for batch / DataSet execution
     */
    public static RuleSet OPT_RULES = RuleSets.ofList(
            // translate to Flink DataSet nodes
//            DataSetWindowAggregateRule.INSTANCE,
//            DataSetAggregateRule.INSTANCE,
//            DataSetDistinctRule.INSTANCE,
//            DataSetCalcRule.INSTANCE,
//            DataSetJoinRule.INSTANCE,
//            DataSetSingleRowJoinRule.INSTANCE,
//            DataSetScanRule.INSTANCE,
//            DataSetUnionRule.INSTANCE,
//            DataSetIntersectRule.INSTANCE,
//            DataSetMinusRule.INSTANCE,
//            DataSetSortRule.INSTANCE,
//            DataSetValuesRule.INSTANCE,
//            DataSetCorrelateRule.INSTANCE,
//            BatchTableSourceScanRule.INSTANCE
    );
}
