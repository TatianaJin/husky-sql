package husky.sql.calcite.plan.logical;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class EnumerableToLogicalTableScan extends RelOptRule {

    public static EnumerableToLogicalTableScan INSTANCE = new EnumerableToLogicalTableScan(operand(EnumerableTableScan.class, any()), "EnumerableToLogicalTableScan");

    public EnumerableToLogicalTableScan(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        EnumerableTableScan oldRel = call.rel(0);
        RelOptTable table = oldRel.getTable();
        LogicalTableScan newRel = LogicalTableScan.create(oldRel.getCluster(), table);
        call.transformTo(newRel);
    }
}
