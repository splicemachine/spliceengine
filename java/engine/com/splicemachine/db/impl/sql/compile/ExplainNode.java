package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainNode extends DMLStatementNode {

    StatementNode node;

    int activationKind() { return StatementNode.NEED_NOTHING_ACTIVATION; }

    public String statementToString() { return "Explain"; }

    public void init(Object statementNode) { node = (StatementNode)statementNode; }

    /**
     * Used by splice. Provides direct access to the node underlying the explain node.
     * @return the root of the actual execution plan.
     */
    @SuppressWarnings("UnusedDeclaration")
    public StatementNode getPlanRoot(){
        return node;
    }

    @Override
    public void optimizeStatement() throws StandardException {
        node.optimizeStatement();
    }

    @Override
    public void bindStatement() throws StandardException {
        node.bindStatement();
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        acb.pushGetResultSetFactoryExpression(mb);
        // parameter
        node.generate(acb, mb);
        acb.pushThisAsActivation(mb);
        int resultSetNumber = getCompilerContext().getNextResultSetNumber();
        mb.push(resultSetNumber);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null, "getExplainResultSet", ClassName.NoPutResultSet, 3);
    }

    @Override
    public ResultDescription makeResultDescription() {
        DataTypeDescriptor dtd = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.VARCHAR_NAME), true);
        ResultColumnDescriptor[] colDescs = new GenericColumnDescriptor[1];
        colDescs[0] = new GenericColumnDescriptor("Plan", dtd);
        String statementType = statementToString();

        return getExecutionFactory().getResultDescription(colDescs, statementType );
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if ( node!= null) {
            node = (StatementNode)node.accept(v);
        }
    }

    @Override
    public ConstantAction makeConstantAction() throws StandardException {
        return	node.makeConstantAction();
    }

}
