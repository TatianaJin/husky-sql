package husky.sql.calcite.plan.nodes.logical.converter;

import husky.sql.calcite.plan.nodes.HuskyConventions;
import husky.sql.calcite.plan.nodes.logical.HuskyLogicalNativeTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class HuskyLogicalNativeTableScanConverter extends ConverterRule {

    public static HuskyLogicalNativeTableScanConverter INSTANCE = new HuskyLogicalNativeTableScanConverter();

    public HuskyLogicalNativeTableScanConverter() {
        super(LogicalTableScan.class, Convention.NONE, HuskyConventions.LOGICAL, "HuskyLogicalNativeTableScanConverter");
    }

    @Override
    public boolean matches(RelOptRuleCall call){
        TableScan scan = call.rel(0);
        return true;
    }

    @Override
    public RelNode convert(RelNode rel) {
        TableScan scan = (TableScan)rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(HuskyConventions.LOGICAL);
        return new HuskyLogicalNativeTableScan(
                rel.getCluster(),
                traitSet,
                scan.getTable()
        );
    }
}
