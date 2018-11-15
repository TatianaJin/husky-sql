package husky.sql.calcite.plan.nodes.logical;

import husky.sql.calcite.plan.rules.logical.PushProjectIntoTableScanRule;
// import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class HuskyLogicalTableScan extends TableScan implements HuskyLogicalRel {
    public int[] fields;

    public HuskyLogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, int[] selectedFields) {
        super(cluster, traitSet, table);
        fields = Arrays.copyOf(selectedFields, selectedFields.length);
        // fields = new int[]{0, 1, 3, 4, 5};
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> allFieldList = table.getRowType().getFieldNames();
        List<String> selectedFieldList = new ArrayList<String>();
        for(int i = 0; i < fields.length; i++) {
            selectedFieldList.add(allFieldList.get(fields[i]) + "=[$" + fields[i] + "]");
        }
        return super.explainTerms(pw)
            .item("fields", selectedFieldList.stream().collect(Collectors.joining(", ")));
            
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        if(fields.length == 0) {
            // no project
            for(RelDataTypeField field : fieldList) {
                builder.add(field);
            }
        } else {
            for(int field : fields) {
                builder.add(fieldList.get(field));
            }
        }
        return builder.build();
    }

    // @Override
    // public void register(RelOptPlanner planner) {
    //     planner.addRule(PushProjectIntoTableScanRule.INSTANCE);
    // }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Multiply the cost by a factor that makes a scan more attractive if it
        // has significantly fewer fields than the original scan.
        //
        // The "+ 2D" on top and bottom keeps the function fairly smooth.
        //
        // For example, if table has 3 fields, project has 1 field,
        // then factor = (1 + 2) / (3 + 2) = 0.6
        Double rowCnt = mq.getRowCount(this);
        planner.getCostFactory().makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType()));
        return super.computeSelfCost(planner, mq)
            .multiplyBy(((double) fields.length + 2D)
            / ((double) table.getRowType().getFieldCount() + 2D));
    }

    // @Override
    public HuskyLogicalTableScan copy(RelTraitSet traitSet, int[] selectedFields) {
        // assert inputs.isEmpty();
        return new HuskyLogicalTableScan(getCluster(), getTraitSet(), getTable(), selectedFields);
    }
}
