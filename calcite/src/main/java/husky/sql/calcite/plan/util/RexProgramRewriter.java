package husky.sql.calcite.plan.util;

import org.apache.calcite.rex.*;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class RexProgramRewriter {
	public static class InputRewriter extends RexShuttle {
		private int[] fields;
		/** old input fields ref index -> new input fields ref index mappings */
		private Map<Integer, Integer> fieldMap;

		InputRewriter(int[] fields) {
			this.fields = fields;
			this.fieldMap = new HashMap<>();
			for(int i = 0; i < fields.length; i++) {
				this.fieldMap.put(fields[i], i);
			}
		}

		@Override
		public RexNode visitInputRef(RexInputRef inputRef) {
			return new RexInputRef(refNewIndex(inputRef), inputRef.getType());
		}

		@Override
		public RexNode visitLocalRef(RexLocalRef localRef) {
			return new RexInputRef(refNewIndex(localRef), localRef.getType());
		}

		private int refNewIndex(RexSlot ref) {
			try{
				return fieldMap.get(ref.getIndex());
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("input field contains invalid index");
			}
		}
	}

	public static RexProgram rewriteWithFieldProjection(RexProgram rexProgram, RelDataType inputRowType, RexBuilder rexBuilder, int[] usedFileds) {
		InputRewriter inputRewriter= new InputRewriter(usedFileds);

		// rewrite input field in projections
		List<RexNode> newProjectExpressions = rexProgram.getProjectList().stream().map(
			exp -> rexProgram.expandLocalRef(exp).accept(inputRewriter)).collect(Collectors.toList());

		// rewrite input field in condition
		RexLocalRef condition = rexProgram.getCondition();
		RexNode newConditionExpression;
		if(condition != null) {
			newConditionExpression = rexProgram.expandLocalRef(condition).accept(inputRewriter);
		} else {
			newConditionExpression = null;
		}

		return RexProgram.create(inputRowType, newProjectExpressions, newConditionExpression, rexProgram.getOutputRowType(), rexBuilder);
	}
}