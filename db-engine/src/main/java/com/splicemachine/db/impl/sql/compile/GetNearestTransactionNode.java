package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.List;

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class GetNearestTransactionNode extends UnaryOperatorNode {
    /**
     * @inheritDoc
     * @throws StandardException if the operand type is not a timestamp.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {

        bindOperand(fromList, subqueryList, aggregateVector);

        TypeId operandType = operand.getTypeId();
        if (operandType.getJDBCTypeId() != Types.TIMESTAMP) {
            throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                    getOperatorString(),
                    operandType.getSQLTypeName());
        }

        // The return type is BIGINT
        TypeId resultTypeId = TypeId.getBuiltInTypeId(Types.BIGINT);
        assert resultTypeId != null;
        setType(new DataTypeDescriptor(resultTypeId, true));
        setOperator("get_nearest_transaction");
        setMethodName("GetNearestTransaction");
        return this;
    }

    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        operand.generateExpression(acb, mb);
        mb.upCast(ClassName.DataValueDescriptor);
        mb.callMethod(VMOpcode.INVOKESTATIC, ClassName.TxnUtil, "getPastTxn", ClassName.NumberDataValue, 1);
    }
}
