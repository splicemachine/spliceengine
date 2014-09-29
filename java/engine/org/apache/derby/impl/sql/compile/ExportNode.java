package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.GenericColumnDescriptor;

import java.util.List;

/**
 * Export Node
 * <p/>
 * EXAMPLE: export select a, b, sqrt(c) from table1 where a > 100;
 */
public class ExportNode extends DMLStatementNode {

    private StatementNode node;
    /* HDFS, local, etc */
    private String exportPath;
    private String fileSystem;
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
    public void init(Object statementNode, Object argumentsVector) {
        List argsList = (List) argumentsVector;
        this.node = (StatementNode) statementNode;
        try {
            this.exportPath = stringValue(argsList.get(0));
            this.fileSystem = stringValue(argsList.get(1));
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
        mb.push(fileSystem);
        mb.push(replicationCount);
        mb.push(encoding);
        mb.push(fieldSeparator);
        mb.push(quoteCharacter);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getExportResultSet", ClassName.NoPutResultSet, 9);
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

}