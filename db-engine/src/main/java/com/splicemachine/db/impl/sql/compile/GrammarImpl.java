package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.util.List;
import java.util.Properties;
import java.util.Vector;

class GrammarImpl {
    public NodeFactory nodeFactory;
    public ContextManager cm;
    public CompilerContext compilerContext;

    // Get the current ContextManager
    public final ContextManager getContextManager()
    {
        return cm;
    }

    /**
     * Set up and like the parameters to the descriptors.
     * Set all the ParameterNodes to point to the array of
     * parameter descriptors.
     *
     *    @exception    StandardException
     */
    private void setUpAndLinkParameters(Vector parameterList)
            throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        cc.setParameterList(parameterList);
        /* Link the untyped parameters to the array of parameter descriptors */

        DataTypeDescriptor[] descriptors = cc.getParameterTypes();

        ParameterNode                newNode;
        int                            paramCount;

        /*
         ** Iterate through the list of untyped parameter nodes, set each one
         ** to point to the array of parameter descriptors.
         */
        paramCount = -1;
        int plSize = parameterList.size();
        for (int index = 0; index < plSize; index++)
        {
            paramCount++;

            newNode = (ParameterNode) parameterList.elementAt(index);
            newNode.setDescriptors(descriptors );
        }
    }

    /**
     * Get a DELETE node given the pieces.
     *
     *
     *    @exception    StandardException
     */
    public StatementNode getDeleteNode(FromTable fromTable,
                                       TableName tableName,
                                       ValueNode whereClause,
                                       Properties targetProperties, Vector parameterList)
            throws StandardException
    {
        FromList   fromList = (FromList) nodeFactory.getNode(
                C_NodeTypes.FROM_LIST,
                getContextManager());

        fromList.addFromTable(fromTable);

        SelectNode resultSet = (SelectNode) nodeFactory.getNode(
                C_NodeTypes.SELECT_NODE,
                null,
                null,     /* AGGREGATE list */
                fromList, /* FROM list */
                whereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager());

        StatementNode retval =
                (StatementNode) nodeFactory.getNode(
                        C_NodeTypes.DELETE_NODE,
                        tableName,
                        resultSet,
                        targetProperties,
                        getContextManager());

        setUpAndLinkParameters(parameterList);

        return retval;
    }

    private CompilerContext getCompilerContext() {
        return compilerContext;
    }

    /**
     * Translate a String containing a number into the appropriate type
     * of Numeric node.
     *
     * @param num      the string containing the number
     * @param intsOnly accept only integers (not decimal)
     *
     * @exception StandardException        Thrown on error
     */
    NumericConstantNode getNumericNode(String num, boolean intsOnly)
            throws StandardException
    {
        ContextManager cm = getContextManager();

        // first, see if it might be an integer
        try
        {
            return (NumericConstantNode) nodeFactory.getNode(
                    C_NodeTypes.INT_CONSTANT_NODE,
                    new Integer(num),
                    cm);
        }
        catch (NumberFormatException nfe)
        {
            // we catch because we want to continue on below
        }

        // next, see if it might be a long
        try
        {
            return (NumericConstantNode) nodeFactory.getNode(
                    C_NodeTypes.LONGINT_CONSTANT_NODE,
                    new Long(num),
                    cm);
        }
        catch (NumberFormatException nfe)
        {
            if (intsOnly) {
                throw nfe;
            }
            // else we want to continue on below
        }

        NumericConstantNode ncn =
                (NumericConstantNode) nodeFactory.getNode(
                        C_NodeTypes.DECIMAL_CONSTANT_NODE,
                        num,
                        cm);
        if (ncn != null) {
            int precision = ncn.getTypeServices().getPrecision();
            if (precision > TypeCompiler.MAX_DECIMAL_PRECISION_SCALE)
                throw StandardException.newException(SQLState.DECIMAL_TOO_MANY_DIGITS);
        }
        return ncn;

    }

    /**
     * Get one of the several types of create alias nodes.
     *
     * @param aliasName    The name of the alias
     * @param targetName TableName for target, or the full path/method name
     * @param aliasSpecificInfo     Information specific to the type of alias being created.
     * @param aliasType    The type of alias to create
     * @return      A CreateAliasNode matching the given parameters
     *
     * @exception StandardException        Thrown on error
     */
    public StatementNode
    getCreateAliasNode(
            TableName aliasName,
            Object targetName,
            Object aliasSpecificInfo,
            char aliasType) throws StandardException
    {
        String methodName = null;

        if (
                (aliasType != AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR) &&
                        (aliasType != AliasInfo.ALIAS_TYPE_UDT_AS_CHAR) &&
                        (aliasType != AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR)
        )
        {
            int lastPeriod;
            String fullStaticMethodName = (String) targetName;
            int paren = fullStaticMethodName.indexOf('(');
            if (paren == -1) {
                // not a Java signature - split based on last period
                lastPeriod = fullStaticMethodName.lastIndexOf('.');
            } else {
                // a Java signature - split on last period before the '('
                lastPeriod = fullStaticMethodName.substring(0, paren).lastIndexOf('.');
            }
            if (lastPeriod == -1 || lastPeriod == fullStaticMethodName.length()-1) {
                throw StandardException.newException(SQLState.LANG_INVALID_FULL_STATIC_METHOD_NAME, fullStaticMethodName);
            }
            String javaClassName = fullStaticMethodName.substring(0, lastPeriod);
            methodName = fullStaticMethodName.substring(lastPeriod + 1);
            targetName = javaClassName;
        }

        return new CreateAliasNode(
                aliasName,
                targetName,
                methodName,
                aliasSpecificInfo,
                aliasType,
                cm );
    }

    /**
     * Get one of the several types of create alias nodes.
     *
     * @param aliasName    The name of the alias
     * @param targetName TableName for target, or the full path/method name
     * @param aliasSpecificInfo     Information specific to the type of alias being created.
     * @param aliasType    The type of alias to create
     * @param delimitedIdentifier    Whether or not to treat the class name
     *                                as a delimited identifier if trying to
     *                                resolve it as a class alias.
     *
     * @return    A CreateAliasNode matching the given parameters
     *
     * @exception StandardException        Thrown on error
     */
    StatementNode
    getCreateAliasNode( // Used for Stored Procedure
                        Object aliasName,
                        Object targetName,
                        Object aliasSpecificInfo,
                        char aliasType,
                        Boolean delimitedIdentifier)
            throws StandardException
    {
        int nodeType = C_NodeTypes.CREATE_ALIAS_NODE;
        String methodName = null;

        if (
                (aliasType != AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR) &&
                        (aliasType != AliasInfo.ALIAS_TYPE_UDT_AS_CHAR) &&
                        (aliasType != AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR)
        )
        {
            if (((String)((Object[]) aliasSpecificInfo)[CreateAliasNode.LANGUAGE]).equals("PYTHON"))
            {
                // For Python Stored Procedure
                methodName = "INVALID_METHOD_NAME";
                targetName = (String) targetName;  // The script is stored inside targetName
            }
            else
            {
                // For Original Java Stored Procedure
                int lastPeriod;
                String fullStaticMethodName = (String) targetName;
                int paren = fullStaticMethodName.indexOf('(');
                if (paren == -1) {
                    // not a Java signature - split based on last period
                    lastPeriod = fullStaticMethodName.lastIndexOf('.');
                } else {
                    // a Java signature - split on last period before the '('
                    lastPeriod = fullStaticMethodName.substring(0, paren).lastIndexOf('.');
                }
                if (lastPeriod == -1 || lastPeriod == fullStaticMethodName.length()-1) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FULL_STATIC_METHOD_NAME, fullStaticMethodName);
                }
                String javaClassName = fullStaticMethodName.substring(0, lastPeriod);
                methodName = fullStaticMethodName.substring(lastPeriod + 1);
                targetName = javaClassName;
            }
        }

        return (StatementNode) getNodeFactory().getNode(
                nodeType,
                aliasName,
                targetName,
                methodName,
                aliasSpecificInfo,
                new Character(aliasType),
                delimitedIdentifier,
                cm );
    }


    /*
     * Generate a multiplicative operator node, if necessary.
     *
     * If there are two operands, generate the multiplicative operator
     * that corresponds to the multiplicativeOperator parameter.  If there
     * is no left operand, just return the right operand.
     *
     * @param leftOperand    The left operand, null if no operator
     * @param rightOperand    The right operand
     * @param multiplicativeOperator    An identifier from BinaryOperatorNode
     *                                    telling what operator to generate.
     *
     * @return    The multiplicative operator, or the right operand if there is
     *            no operator.
     *
     * @exception StandardException        Thrown on error
     */
    public ValueNode multOp(ValueNode leftOperand,
                     ValueNode rightOperand,
                     int multiplicativeOperator)
            throws StandardException
    {
        if (leftOperand == null)
        {
            return rightOperand;
        }

        switch (multiplicativeOperator)
        {
            case BinaryOperatorNode.TIMES:
                return (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.BINARY_TIMES_OPERATOR_NODE,
                        leftOperand,
                        rightOperand,
                        getContextManager());

            case BinaryOperatorNode.DIVIDE:
                return (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE,
                        leftOperand,
                        rightOperand,
                        getContextManager());
            case BinaryOperatorNode.CONCATENATE:
                return (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.CONCATENATION_OPERATOR_NODE,
                        leftOperand,
                        rightOperand,
                        getContextManager());

            default:
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Unexpected multiplicative operator " +
                            multiplicativeOperator);
                return null;
        }
    }

    /**
     * Get an UPDATE node given the pieces.
     *
     *
     *    @exception    StandardException
     */
    public StatementNode getUpdateNode(FromTable fromTable,
                                        TableName tableName,
                                        ResultColumnList setClause,
                                        ValueNode whereClause,
                                        Vector parameterList)
            throws StandardException
    {
        FromList   fromList = (FromList) nodeFactory.getNode(
                C_NodeTypes.FROM_LIST,
                getContextManager());

        fromList.addFromTable(fromTable);

        SelectNode resultSet = (SelectNode) nodeFactory.getNode(
                C_NodeTypes.SELECT_NODE,
                setClause, /* SELECT list */
                null,     /* AGGREGATE list */
                fromList, /* FROM list */
                whereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager());

        StatementNode retval =
                (StatementNode) nodeFactory.getNode(
                        C_NodeTypes.UPDATE_NODE,
                        tableName,
                        resultSet,
                        getContextManager());

        setUpAndLinkParameters(parameterList);

        return retval;
    }

    public StatementNode getUpdateNodeWithSub(FromTable fromTable, /* table to be updated */
                                               TableName tableName, /* table to be updated */
                                               ResultColumnList setClause, /* new values to ue for the update */
                                               ValueNode whereClause, /* where clause for outer update */
                                               ValueNode subQuery, /* inner source subquery for multi column syntax */
                                               Vector parameterList)
            throws StandardException
    {
        FromList   fromList = (FromList) nodeFactory.getNode(
                C_NodeTypes.FROM_LIST,
                getContextManager());

        fromList.addFromTable(fromTable);

        // Bring the subquery table(s) to the outer from list
        SelectNode innerSelect = (SelectNode)((SubqueryNode)subQuery).getResultSet();
        FromList innerFrom = innerSelect.getFromList();
        List innerFromEntries = innerFrom.getNodes();
        for (Object obj : innerFromEntries) {
            assert obj instanceof FromTable;
            fromList.addFromTable((FromTable)obj);
        }

        // Bring the subquery where clause to outer where clause
        ValueNode innerWhere = innerSelect.getWhereClause();
        ValueNode alteredWhereClause;
        if (whereClause != null) {
            alteredWhereClause = (ValueNode) getNodeFactory().getNode(
                    C_NodeTypes.AND_NODE,
                    whereClause, /* the one passed into this method */
                    innerWhere,  /* the one pulled from subquery */
                    getContextManager());
        } else {
            alteredWhereClause = innerWhere;
        }

        // Alter the passed in setClause to give it non-null expressions
        // and to point to subqjuery table.
        ResultColumnList innerRCL = innerSelect.getResultColumns();
        int inputSize = setClause.size();
        for (int index = 0; index < inputSize; index++)
        {
            ResultColumn rc = ((ResultColumn) setClause.elementAt(index));
            // String columnName = rc.getName();
            ResultColumn innerResultColumn = ((ResultColumn)innerRCL.elementAt(index));
            String innerColumnName = innerResultColumn.getName();
            if (innerColumnName != null) {
                // source is a case of single column
                ValueNode colRef = (ValueNode) getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        innerColumnName,
                        ((FromTable)innerFromEntries.get(0)).getTableName(),
                        getContextManager());
                rc.setExpression(colRef);
            } else {
                // source is an expression
                rc.setExpression(innerResultColumn.getExpression());
            }
        }

        SelectNode resultSet = (SelectNode) nodeFactory.getNode(
                C_NodeTypes.SELECT_NODE,
                setClause, /* SELECT list */
                null,   /* AGGREGATE list */
                fromList, /* FROM list */
                alteredWhereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager());

        StatementNode retval =
                (StatementNode) nodeFactory.getNode(
                        C_NodeTypes.UPDATE_NODE,
                        tableName, /* target table for update */
                        resultSet, /* SelectNode just created */
                        getContextManager());

        ((UpdateNode)retval).setUpdateWithSubquery(true);

        setUpAndLinkParameters(parameterList);

        return retval;
    }

    private NodeFactory getNodeFactory() { return nodeFactory; }

}