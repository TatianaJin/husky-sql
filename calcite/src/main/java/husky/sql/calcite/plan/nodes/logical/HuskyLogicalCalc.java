package husky.sql.calcite.plan.nodes.logical;

import husky.sql.calcite.plan.nodes.CommonCalc;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;

public class HuskyLogicalCalc extends Calc implements HuskyLogicalRel, CommonCalc {


    public HuskyLogicalCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram calcProgram) {
        super(cluster, traitSet, input, calcProgram);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new HuskyLogicalCalc(getCluster(), traitSet, child, program);
    }

}
