package husky.sql.calcite.plan.util;

import org.apache.calcite.rex.*;

import java.util.LinkedHashSet;
import java.util.Arrays;

public class InputRefVisitor extends RexVisitorImpl<Void> {
	private LinkedHashSet<Integer> fields;

	InputRefVisitor() {
		super(true);
		this.fields = new LinkedHashSet<Integer>();
	}

	public int[] getFields() {
		return Arrays.stream(this.fields.toArray(new Integer[0])).mapToInt(Integer::intValue).toArray();
	}

	@Override
	public Void visitInputRef(RexInputRef inputRef) {
		this.fields.add(inputRef.getIndex());
		return null;
	}

	@Override
	public Void visitCall(RexCall call) {
		call.operands.forEach(operand -> operand.accept(this));
		return null;
	}
}