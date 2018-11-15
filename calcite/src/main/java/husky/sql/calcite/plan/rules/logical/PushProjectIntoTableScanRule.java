package husky.sql.calcite.plan.rules.logical;

import husky.sql.calcite.plan.util.*;
import husky.sql.calcite.plan.nodes.logical.HuskyLogicalTableScan;
import husky.sql.calcite.plan.nodes.logical.HuskyLogicalCalc;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;
import java.util.ArrayList;

public class PushProjectIntoTableScanRule extends RelOptRule {
    public static final PushProjectIntoTableScanRule INSTANCE =
      new PushProjectIntoTableScanRule();

    public PushProjectIntoTableScanRule() {
        super(operand(HuskyLogicalCalc.class, 
            operand(HuskyLogicalTableScan.class, none())), 
            "PushProjectIntoTableScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        HuskyLogicalTableScan scan = call.rel(1);
        
        // only continue if we haven't pushed down a projection yet.
        return scan.fields.length == 0;
    }

    @Override
    public void onMatch(RelOptRuleCall call) { 
        HuskyLogicalCalc calc = call.rel(0);
        HuskyLogicalTableScan scan = call.rel(1);
        int[] fields = RexProgramExtractor.extractRefInputFields(calc.getProgram());
        
        HuskyLogicalTableScan newScan = scan.copy(scan.getTraitSet(), fields);
        RexProgram newCalProgram = RexProgramRewriter.rewriteWithFieldProjection(
            calc.getProgram(), newScan.getRowType(), 
            calc.getCluster().getRexBuilder(), fields);

        if(newCalProgram.isTrivial()) {
            // drop calc if the transformed program merely returns its input and doesn't exist filter
            call.transformTo(newScan);
        } else {
            call.transformTo(calc.copy(calc.getTraitSet(), newScan, newCalProgram));
        }

        // call.transformTo(
        //     new HuskyLogicalTableScan(
        //         scan.getCluster(),
        //         scan.getTraitSet(),
        //         scan.getTable(),
        //         fields));
    }

    private int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];
        for(int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if(exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}