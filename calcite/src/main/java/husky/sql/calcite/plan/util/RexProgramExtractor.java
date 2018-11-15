package husky.sql.calcite.plan.util;

import org.apache.calcite.rex.*;

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
}