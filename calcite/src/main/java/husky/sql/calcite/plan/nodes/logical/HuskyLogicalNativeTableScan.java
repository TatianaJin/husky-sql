package husky.sql.calcite.plan.nodes.logical;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

public class HuskyLogicalNativeTableScan extends TableScan implements HuskyLogicalRel {

    public HuskyLogicalNativeTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        Double rowCnt = mq.getRowCount(this);
        planner.getCostFactory().makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType()));
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new HuskyLogicalNativeTableScan(getCluster(), getTraitSet(), getTable());
    }
}
