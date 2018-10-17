package husky.sql.calcite.plan.nodes;


import husky.sql.calcite.Exceptions.TableException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public interface HuskyRelNode extends RelNode {

    default String getExpressionString(RexNode expr, List<String> inFields, List<RexNode> localExprsTable) {
        String res;
        if (expr instanceof RexInputRef) {
            RexInputRef i = (RexInputRef) expr;
            res = inFields.get(i.getIndex());
        } else if (expr instanceof RexLiteral) {
            RexLiteral i = (RexLiteral) expr;
            res = i.toString();
        } else if (expr instanceof RexLocalRef) {
            RexLocalRef i = (RexLocalRef) expr;
            if (localExprsTable == null) {
                throw new IllegalArgumentException("Encountered RexLocalRef without " +
                        "local expression table");
            } else {
                RexNode lExpr = localExprsTable.get(i.getIndex());
                res = getExpressionString(lExpr, inFields, localExprsTable);
            }
        } else if (expr instanceof RexCall) {
            RexCall c = (RexCall) expr;
            String op = c.getOperator().toString();
            List<String> ops = new ArrayList<>();
            for (RexNode rex : c.getOperands()) {
                ops.add(getExpressionString(rex, inFields, localExprsTable));
            }
            if (c.getOperator() instanceof SqlAsOperator) {
                res = ops.get(0);
            } else {
                res = op + "(" + String.join(",", ops) + ")";
            }
        } else if (expr instanceof RexFieldAccess) {
            RexFieldAccess fa = (RexFieldAccess) expr;
            String referenceExpr = getExpressionString(fa.getReferenceExpr(), inFields, localExprsTable);
            String field = fa.getField().getName();
            res = referenceExpr + "." + field;
        } else if (expr instanceof RexCorrelVariable) {
            RexCorrelVariable cv = (RexCorrelVariable) expr;
            res = cv.toString();
        } else {
            throw new IllegalArgumentException("Unknown expression type " + expr.getClass() + ":" + expr);
        }
        return res;
    }

    default Double estimateRowSize(RelDataType rowType) {
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        Double accum = 0.0;
        for (RelDataTypeField f : fieldList) {
            accum += estimateDataTypeSize(f.getType());
        }

        return accum;
    }

    default Double estimateDataTypeSize(RelDataType t) {
        double res = 0.0;
        if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(t.getSqlTypeName())) {
            res = 8;
        } else if (SqlTypeName.DAY_INTERVAL_TYPES.contains(t.getSqlTypeName())) {
            res = 4;
        } else {
            switch (t.getSqlTypeName()) {
                case TINYINT:
                    res = 1;
                    break;
                case SMALLINT:
                    res = 2;
                    break;
                case INTEGER:
                    res = 4;
                    break;
                case BIGINT:
                    res = 8;
                    break;
                case BOOLEAN:
                    res = 1;
                    break;
                case FLOAT:
                    res = 4;
                    break;
                case DOUBLE:
                    res = 8;
                    break;
                case VARCHAR:
                    res = 12;
                    break;
                case CHAR:
                    res = 1;
                    break;
                case DECIMAL:
                    res = 12;
                    break;
                case TIME:
                case TIMESTAMP:
                case DATE:
                    res = 12;
                    break;
                case ROW:
                    res = estimateRowSize(t);
                    break;
                case ARRAY:
                    // 16 is an arbitrary estimate
                    res = estimateDataTypeSize(t.getComponentType()) * 16;
                    break;
                case MAP:
                case MULTISET:
                    // 16 is an arbitrary estimate
                    res = (estimateDataTypeSize(t.getKeyType()) + estimateDataTypeSize(t.getValueType())) * 16;
                    break;
                case ANY:
                    res = 128;
                    break;
                default:
                    throw new TableException("Unsupported data type encountered: " + t);
            }
        }
        return res;
    }

}
