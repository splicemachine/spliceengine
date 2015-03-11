package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;

import java.util.List;

/**
 * Export Node
 * <p/>
 * EXAMPLE:
 * <p/>
 * EXPORT('/dir', true, 3, 'utf-8', ',', '"') select a, b, sqrt(c) from table1 where a > 100;
 */
public class ExportNode extends DMLStatementNode {

    private static final int EXPECTED_ARGUMENT_COUNT = 6;

    private StatementNode node;
    /* HDFS, local, etc */
    private String exportPath;
    private boolean compression;
    private int replicationCount;
    private String encoding;
    private String fieldSeparator;
    private String quoteCharacter;

    @Override
    int activationKind() {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }

    @Override
    public String statementToString() {
        return "Export";
    }

    @Override
    public void init(Object statementNode, Object argumentsVector) throws StandardException {
        if (!(argumentsVector instanceof List) || ((List) argumentsVector).size() != EXPECTED_ARGUMENT_COUNT) {
            throw StandardException.newException(SQLState.LANG_DB2_NUMBER_OF_ARGS_INVALID, "EXPORT");
        }
        List argsList = (List) argumentsVector;
        this.node = (StatementNode) statementNode;
        try {
            this.exportPath = stringValue(argsList.get(0));
            this.compression = booleanValue(argsList.get(1));
            this.replicationCount = intValue(argsList.get(2));
            this.encoding = stringValue(argsList.get(3));
            this.fieldSeparator = stringValue(argsList.get(4));
            this.quoteCharacter = stringValue(argsList.get(5));
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
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
        mb.push(exportPath);
        mb.push(compression);
        mb.push(replicationCount);
        mb.push(encoding);
        mb.push(fieldSeparator);
        mb.push(quoteCharacter);

        /* Save result description of source node for use in export formatting. */
        mb.push(acb.addItem(node.makeResultDescription()));

        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getExportResultSet", ClassName.NoPutResultSet, 10);
    }

    @Override
    public ResultDescription makeResultDescription() {
        DataTypeDescriptor dtd1 = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.LONGINT_NAME), true);
        DataTypeDescriptor dtd2 = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.LONGINT_NAME), true);
        ResultColumnDescriptor[] columnDescriptors = new GenericColumnDescriptor[2];
        columnDescriptors[0] = new GenericColumnDescriptor("Row Count", dtd1);
        columnDescriptors[1] = new GenericColumnDescriptor("Total Time (ms)", dtd2);
        String statementType = statementToString();
        return getExecutionFactory().getResultDescription(columnDescriptors, statementType);
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (node != null) {
            node = (StatementNode) node.accept(v);
        }
    }

    private static boolean isNullConstant(Object object) {
        return object instanceof ConstantNode && ((ConstantNode) object).isNull();
    }

    private static String stringValue(Object object) throws StandardException {
        /* MethodBuilder can't handle null, so we use empty string when the user types NULL as argument */
        return isNullConstant(object) ? "" : ((CharConstantNode) object).getString();
    }

    private static int intValue(Object object) throws StandardException {
        return isNullConstant(object) ? -1 : ((NumericConstantNode) object).getValue().getInt();
    }

    private static boolean booleanValue(Object object) throws StandardException {
        return isNullConstant(object) ? true : ((BooleanConstantNode) object).isBooleanTrue();
    }

}