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
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.metadata.RelMdUtil;

public class HuskyLogicalCalc extends Calc implements HuskyLogicalRel, CommonCalc {
	private RexProgram calcProgram;

    public HuskyLogicalCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram calcProgram) {
        super(cluster, traitSet, input, calcProgram);
        this.calcProgram = calcProgram;
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new HuskyLogicalCalc(getCluster(), traitSet, child, program);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    	RelNode child = this.getInput();
    	double rowCnt = mq.getRowCount(child);
    	return computeSelfCost(calcProgram, planner, rowCnt);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery metadata) {
    	RelNode child = this.getInput();
    	double rowCnt = metadata.getRowCount(child);
    	return estimateRowCount(calcProgram, rowCnt);
    }

    private RelOptCost computeSelfCost(RexProgram calcProgram, RelOptPlanner planner, double rowCnt) {
		// compute number of expressions that do not access a field or literal, i.e. computations,
	    // conditions, etc. We only want to account for computations, not for simple projections.
	    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
	    // in normalization stage. So we should ignore CASTs here in optimization stage.
	    // Also, we add 1 to take calc RelNode number into consideration, so the cost of merged calc
	    // RelNode will be less than the total cost of un-merged calcs.
	    
	    double compCnt = calcProgram.getExprList().stream().filter(rexNode -> isComputation(rexNode)).count() + 1;
	    double newRowCnt = estimateRowCount(calcProgram, rowCnt);;

	    return planner.getCostFactory().makeCost(newRowCnt, newRowCnt * compCnt, 0);
	}

	private double estimateRowCount(RexProgram calcProgram, double rowCnt) {
		if(calcProgram.getCondition() != null) {
			// we reduce the result card to push filters down
			RexNode exprs = calcProgram.expandLocalRef(calcProgram.getCondition());
			double selectivity = RelMdUtil.guessSelectivity(exprs, false);
			return Math.max(rowCnt * selectivity, 1.0);
		} else {
			return rowCnt;
		}
	}

	/**
    * Return true if the input rexNode do not access a field or literal, i.e. computations,
    * conditions, etc.
    */
	private boolean isComputation(RexNode rexNode) {
		if(rexNode instanceof RexInputRef) {
			// Variable which references a field of an input relational expression.
			return false;
		} else if(rexNode instanceof RexLiteral) {
			// Constant value in a row-expression (BOOLEAN, DATE, DECIMAL, ...).
			return false;
		} else if (rexNode instanceof RexCall) {
			if(((RexCall)rexNode).getOperator().getName().equals("CAST")) {
				return false;
			} else {
				return true;
			}
		} else {
			return true;
		}
	}
}
