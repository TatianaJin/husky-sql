package husky.sql.calcite.plan.util;

import husky.sql.calcite.Exceptions.TableException;

import org.apache.calcite.rex.*;
import org.apache.calcite.plan.RelOptUtil;

import java.util.List;
import java.util.ArrayList;

public class RexProgramExtractor {
	public static int[] extractRefInputFields(RexProgram rexProgram) {
		InputRefVisitor visitor = new InputRefVisitor();

		// extract referenced input fields from projections
		rexProgram.getProjectList().forEach(
			exp -> rexProgram.expandLocalRef(exp).accept(visitor)
		);

		// extract referenced input fields from condition
		RexLocalRef condition = rexProgram.getCondition();
		if(condition != null) {
			rexProgram.expandLocalRef(condition).accept(visitor);
		}

		return visitor.getFields();
	}
 
	public static List<RexNode> extractConjunctiveConditions(RexProgram rexProgram, RexBuilder rexBuilder) {
		RexLocalRef condition = rexProgram.getCondition();
		if(condition != null){
			RexNode expanded = rexProgram.expandLocalRef(condition);
			// converts the expanded expression to conjunctive normal form,
        	// like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
        	RexNode cnf = RexUtil.toCnf(rexBuilder, expanded);
        	// converts the cnf condition to a list of AND conditions
        	List<RexNode> conjunctions = RelOptUtil.conjunctions(cnf);

        	return conjunctions;
		} else {
			return new ArrayList<RexNode>();
		}
	}
}