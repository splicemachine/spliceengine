package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * Created by jyuan on 6/9/14.
 */
public class ExplainNode extends DMLStatementNode {

    StatementNode node;

    int activationKind() {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }

    public String statementToString() {
        return "Explain";
    }

    public void init(Object statementNode) {
        node = (StatementNode)statementNode;
    }

    @Override
    public void optimizeStatement() throws StandardException {
        node.optimizeStatement();
    }

    @Override
    public void bindStatement() throws StandardException
    {
        node.bindStatement();
    }

    @Override
    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
            throws StandardException
    {
        acb.pushGetResultSetFactoryExpression(mb);
        // parameter
        node.generate(acb, mb);
        acb.pushThisAsActivation(mb);
        int resultSetNumber = getCompilerContext().getNextResultSetNumber();
        mb.push(resultSetNumber);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getExplainResultSet", ClassName.NoPutResultSet, 3);
    }

    @Override
    public ResultDescription makeResultDescription()
    {
        DataTypeDescriptor dtd = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.VARCHAR_NAME), true);
        ResultColumnDescriptor[] colDescs = new GenericColumnDescriptor[1];
        colDescs[0] = new GenericColumnDescriptor("Plan", dtd);
        String statementType = statementToString();

        return getExecutionFactory().getResultDescription(
                colDescs, statementType );
    }

    void acceptChildren(Visitor v)
            throws StandardException
    {
        super.acceptChildren(v);

        if ( node!= null)
        {
            node = (StatementNode)node.accept(v);
        }
    }

    public StatementNode getExplainPlanRoot() {
        return node;
    }
}
