package husky.sql.calcite.plan.nodes.logical.converter;

import husky.sql.calcite.plan.nodes.HuskyConventions;
import husky.sql.calcite.plan.nodes.logical.HuskyLogicalCalc;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;

public class HuskyLogicalCalcConverter extends ConverterRule {

    public static HuskyLogicalCalcConverter INSTANCE = new HuskyLogicalCalcConverter();

    public HuskyLogicalCalcConverter() {
        super(LogicalCalc.class, Convention.NONE, HuskyConventions.LOGICAL, "HuskyLogicalCalcConverter");
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalCalc calc = (LogicalCalc)rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(HuskyConventions.LOGICAL);
        RelNode newInput = RelOptRule.convert(calc.getInput(), HuskyConventions.LOGICAL);

        return new HuskyLogicalCalc(rel.getCluster(), traitSet, newInput, calc.getProgram());
    }
}