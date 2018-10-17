package husky.sql.calcite.plan.nodes.logical.converter;

import husky.sql.calcite.Schema.TableSourceTable;
import husky.sql.calcite.Sources.TableSource;
import husky.sql.calcite.plan.nodes.HuskyConventions;
import husky.sql.calcite.plan.nodes.logical.HuskyLogicalTableSourceScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class HuskyLogicalTableSourceScanConverter extends ConverterRule {

    public static HuskyLogicalTableSourceScanConverter INSTANCE = new HuskyLogicalTableSourceScanConverter();

    public HuskyLogicalTableSourceScanConverter() {
        super(LogicalTableScan.class, Convention.NONE, HuskyConventions.LOGICAL, "HuskyLogicalTableSourceScanConverter");

    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        TableSourceTable<?> tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return false;
        }
        return true;
    }

    @Override
    public RelNode convert(RelNode rel) {
        TableScan scan = (TableScan) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(HuskyConventions.LOGICAL);
        TableSource<?> tableSource = scan.getTable().unwrap(TableSourceTable.class).getTableSource();

        return new HuskyLogicalTableSourceScan(
                rel.getCluster(),
                traitSet,
                scan.getTable(),
                tableSource,
                null
        );
    }
}
