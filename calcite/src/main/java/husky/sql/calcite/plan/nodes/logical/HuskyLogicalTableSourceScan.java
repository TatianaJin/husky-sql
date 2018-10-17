package husky.sql.calcite.plan.nodes.logical;

import husky.sql.calcite.Sources.TableSource;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class HuskyLogicalTableSourceScan extends TableScan implements HuskyLogicalRel {

    public HuskyLogicalTableSourceScan(RelOptCluster cluster,
                                       RelTraitSet traitSet,
                                       RelOptTable table,
                                       TableSource<?> tableSource,
                                       int[] selectedFields) {
        super(cluster, traitSet, table);
    }



}
