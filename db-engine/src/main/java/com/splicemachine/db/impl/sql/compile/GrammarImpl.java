package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableProperties;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;

import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

class GrammarImpl {
    // Define for UTF8 max
    private static final int    MAX_UTF8_LENGTH = 65535;
    static final String SINGLEQUOTES = "\'\'";
    static final String DOUBLEQUOTES = "\"\"";

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

        ParameterNode newNode;
        int paramCount;

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


    /**
     * check if the type length is ok for the given type.
     */
    public static void checkTypeLimits(int type, int length)
            throws StandardException
    {
        boolean valid = true;


        switch (type) {
            case Types.BINARY:
            case Types.CHAR:
                if (length  > Limits.DB2_CHAR_MAXWIDTH)
                    valid = false;
                break;

            case Types.VARBINARY:
            case Types.VARCHAR:
                if (length  > Limits.DB2_VARCHAR_MAXWIDTH)
                    valid = false;

                break;
            default:
                break;
        }
        if (!valid)  // If these limits are too big
        {
            DataTypeDescriptor charDTD =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(type, length);

            throw StandardException.newException(SQLState.LANG_DB2_LENGTH_PRECISION_SCALE_VIOLATION, charDTD.getSQLstring());
        }
    }

    public static void verifyImageLength(String image) throws StandardException
    {
        // beetle 2758.  For right now throw an error for literals > 64K
        if (image.length() > MAX_UTF8_LENGTH)
        {
            throw StandardException.newException(SQLState.LANG_INVALID_LITERAL_LENGTH);
        }
    }

    /*
     ** Converts a delimited id to a canonical form.
     ** Post process delimited identifiers to eliminate leading and
     ** trailing " and convert all occurrences of "" to ".
     */
    public static String normalizeDelimitedID(String str)
    {
        str = StringUtil.compressQuotes(str, DOUBLEQUOTES);
        return str;
    }
    public static boolean isDATETIME(int val)
    {
        if (val == Types.DATE || val == Types.TIME || val == Types.TIMESTAMP)
            return true;
        else
            return false;
    }


    /**
     * Generate a trim operator node
     * @param trimSpec one of Leading, Trailing or Both.
     * @param trimChar the character to trim. Can be null in which case it defaults
     * to ' '.
     * @param trimSource expression to be trimmed.
     */
    public ValueNode getTrimOperatorNode(Integer trimSpec, ValueNode trimChar,
                                          ValueNode trimSource, ContextManager cm) throws StandardException
    {
        if (trimChar == null)
        {
            trimChar = (CharConstantNode) nodeFactory.getNode(
                    C_NodeTypes.CHAR_CONSTANT_NODE,
                    " ",
                    getContextManager());
        }
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.TRIM_OPERATOR_NODE,
                trimSource, // receiver
                trimChar,   // leftOperand.
                null,
                ReuseFactory.getInteger(TernaryOperatorNode.TRIM),
                trimSpec,
                cm == null ? getContextManager() : cm);
    }

    /**
     * Get a substring node from
     *      - the string
     *      - the start position
     *      - the length
     *      - a boolean values for specifying the kind of substring function
     * @exception StandardException  Thrown on error
     */
    public ValueNode getSubstringNode( ValueNode stringValue, ValueNode startPosition,
                                ValueNode length, Boolean boolVal ) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.SUBSTRING_OPERATOR_NODE,
                stringValue,
                startPosition,
                length,
                ReuseFactory.getInteger(TernaryOperatorNode.SUBSTRING),
                null,
                getContextManager());
    }

    /**
     Create a node for the drop alias/procedure call.
     */
    public StatementNode dropAliasNode(Object aliasName, char type) throws StandardException
    {

        StatementNode stmt = (StatementNode) nodeFactory.getNode(
                C_NodeTypes.DROP_ALIAS_NODE,
                aliasName,
                new Character(type),
                getContextManager());

        return stmt;
    }

    /**
     * Splits a given string based on a delimiter and pick out the desired field from the string, start from the left of the string:
     *      - the string
     *      - the delimiter
     *      - the field number
     * @exception StandardException  Thrown on error
     */
    public ValueNode getSplitPartNode(ValueNode stringValue, ValueNode delimiter, ValueNode fieldNumber ) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.SPLIT_PART_OPERATOR_NODE,
                stringValue,
                delimiter,
                fieldNumber,
                ReuseFactory.getInteger(TernaryOperatorNode.SPLIT_PART),
                null,
                getContextManager());
    }

    public ValueNode getRightOperatorNode(ValueNode stringValue, ValueNode length) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.RIGHT_OPERATOR_NODE,
                stringValue,
                length,
                null,
                ReuseFactory.getInteger(TernaryOperatorNode.RIGHT),
                null,
                getContextManager());
    }

    /**
     * Gets a left() node from
     *      - the string
     *      - the length to take
     *      - the padding character
     * @exception StandardException  Thrown on error
     */
    public ValueNode getLeftOperatorNode(ValueNode stringValue, ValueNode length) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.LEFT_OPERATOR_NODE,
                stringValue,
                length,
                null,
                ReuseFactory.getInteger(TernaryOperatorNode.LEFT),
                null,
                getContextManager());
    }

    /**
     * Gets a replace node based on the specified arguments.
     * @param stringValue the input string
     * @param fromString the from sub string
     * @param toString the to string
     * @exception StandardException thrown on error
     */
    public ValueNode getReplaceNode(
            ValueNode stringValue,
            ValueNode fromString,
            ValueNode toString) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.REPLACE_OPERATOR_NODE,
                stringValue,
                fromString,
                toString,
                ReuseFactory.getInteger(TernaryOperatorNode.REPLACE),
                null,
                getContextManager());
    }

    public ValueNode getJdbcIntervalNode( int intervalType) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode( C_NodeTypes.INT_CONSTANT_NODE,
                ReuseFactory.getInteger( intervalType),
                getContextManager());
    }


    public ValueNode convertToTimestampAdd(ValueNode base, TimeSpanNode timeSpanNode, boolean negate) throws StandardException
    {
        ValueNode intervalType = null;
        ValueNode value = null;
        switch (timeSpanNode.getUnit()) {
            case DateTimeDataValue.DAY_INTERVAL:
                intervalType = getJdbcIntervalNode(DateTimeDataValue.DAY_INTERVAL);
                break;
            case DateTimeDataValue.MONTH_INTERVAL:
                intervalType = getJdbcIntervalNode( DateTimeDataValue.MONTH_INTERVAL);
                break;
            case DateTimeDataValue.YEAR_INTERVAL:
                intervalType = getJdbcIntervalNode( DateTimeDataValue.YEAR_INTERVAL);
                break;
            case DateTimeDataValue.HOUR_INTERVAL:
                intervalType = getJdbcIntervalNode( DateTimeDataValue.HOUR_INTERVAL);
                break;
            case DateTimeDataValue.MINUTE_INTERVAL:
                intervalType = getJdbcIntervalNode( DateTimeDataValue.MINUTE_INTERVAL);
                break;
            case DateTimeDataValue.SECOND_INTERVAL:
                intervalType = getJdbcIntervalNode( DateTimeDataValue.SECOND_INTERVAL);
                break;
            default:
                throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION,
                        timeSpanNode.getUnit());
        }
        value = timeSpanNode.getValue();
        if (negate) {
            value = (ValueNode) nodeFactory.getNode(
                    C_NodeTypes.UNARY_MINUS_OPERATOR_NODE,
                    value,
                    getContextManager());
        }
        return (ValueNode) nodeFactory.getNode( C_NodeTypes.TIMESTAMP_ADD_FN_NODE,
                base,
                intervalType,
                value,
                ReuseFactory.getInteger( TernaryOperatorNode.TIMESTAMPADD),
                null,
                getContextManager());

    }

    public ValueNode
    multiplicativeExpression(ValueNode farLeftOperand, int additiveOperator,
                             int multOp, ValueNode leftOperand) throws StandardException
    {
        if (farLeftOperand == null)
            return leftOperand;

        // manipulation for current timestamp operation
        if ((farLeftOperand instanceof CurrentDatetimeOperatorNode) &&
                ((CurrentDatetimeOperatorNode)farLeftOperand).isCurrentTimestamp()) {
            switch (additiveOperator)
            {

                case BinaryOperatorNode.PLUS:
                    if (leftOperand instanceof TimeSpanNode) {
                        return convertToTimestampAdd(farLeftOperand, (TimeSpanNode) leftOperand, false);
                    }
                    return (ValueNode) nodeFactory.getNode(
                            C_NodeTypes.BINARY_PLUS_OPERATOR_NODE,
                            farLeftOperand,
                            leftOperand,
                            getContextManager()
                    );
                case BinaryOperatorNode.MINUS:
                    if (leftOperand instanceof TimeSpanNode) {
                        return convertToTimestampAdd(farLeftOperand, (TimeSpanNode) leftOperand, true);
                    }
                    return (ValueNode) nodeFactory.getNode(
                            C_NodeTypes.BINARY_MINUS_OPERATOR_NODE,
                            farLeftOperand,
                            leftOperand,
                            getContextManager()
                    );

                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT(
                                "Unexpected operator value of " + additiveOperator);
                    return null;
            }
        } else {
            switch (additiveOperator)
            {
                case BinaryOperatorNode.PLUS:
                    if (leftOperand instanceof TimeSpanNode) {
                        return convertAddTimeIntervalToFunction(farLeftOperand, (TimeSpanNode) leftOperand, false);
                    }
                    return (ValueNode) nodeFactory.getNode(
                            C_NodeTypes.BINARY_PLUS_OPERATOR_NODE,
                            farLeftOperand,
                            leftOperand,
                            getContextManager()
                    );
                case BinaryOperatorNode.MINUS:
                    if (leftOperand instanceof TimeSpanNode) {
                        return convertAddTimeIntervalToFunction(farLeftOperand, (TimeSpanNode) leftOperand, true);
                    }
                    return (ValueNode) nodeFactory.getNode(
                            C_NodeTypes.BINARY_MINUS_OPERATOR_NODE,
                            farLeftOperand,
                            leftOperand,
                            getContextManager()
                    );

                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT(
                                "Unexpected operator value of " + additiveOperator);
                    return null;
            }
        }
    }

    public ValueNode
    convertAddTimeIntervalToFunction(ValueNode base, TimeSpanNode timeSpanNode, boolean negate) throws StandardException
    {
        Vector parameterList = new Vector();
        String function;
        switch (timeSpanNode.getUnit()) {
            case DateTimeDataValue.DAY_INTERVAL:
                function = "ADD_DAYS";
                break;
            case DateTimeDataValue.MONTH_INTERVAL:
                function = "ADD_MONTHS";
                break;
            case DateTimeDataValue.YEAR_INTERVAL:
                function = "ADD_YEARS";
                break;
            default:
                throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION,
                        timeSpanNode.getUnit());
        }
        MethodCallNode methodNode = (MethodCallNode) nodeFactory.getNode(
                C_NodeTypes.STATIC_METHOD_CALL_NODE,
                (TableName) nodeFactory.getNode(
                        C_NodeTypes.TABLE_NAME,
                        null,
                        function,
                        getContextManager()
                ),
                null,
                getContextManager()
        );
        parameterList.addElement(base);
        ValueNode value = timeSpanNode.getValue();
        if (negate) {
            value = (ValueNode) nodeFactory.getNode(
                    C_NodeTypes.UNARY_MINUS_OPERATOR_NODE,
                    value,
                    getContextManager());
        }
        parameterList.addElement(value);
        methodNode.addParms(parameterList);
        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
                methodNode,
                getContextManager());
    }

    /**
     * For temporary table validation and creation.
     */
    public StatementNode verifySyntaxAndCreate(Object[] declareTableClauses,
                                                Boolean isGlobal,
                                                TableName tableName,
                                                TableElementList tableElementList,
                                                Integer createBehavior) throws StandardException {
        // NOT LOGGED is allways true
        // if ON COMMIT behavior not explicitly specified in DECLARE command, default to ON COMMIT PRESERVE ROWS
        if (declareTableClauses[1] == null) {
            declareTableClauses[1] = Boolean.FALSE;
        } else if (declareTableClauses[1] == Boolean.TRUE) {
            // ON COMMIT DELETE ROWS is not supported
            throw StandardException.newException(SQLState.LANG_TEMP_TABLE_DELETE_ROWS_NO_SUPPORTED, "COMMIT");
        }
        // if ON ROLLBACK behavior not explicitly specified in DECLARE command, default to ON ROLLBACK DELETE ROWS
        if (declareTableClauses[2] == Boolean.TRUE) {
            // ON ROLLBACK DELETE ROWS is not supported
            throw StandardException.newException(SQLState.LANG_TEMP_TABLE_DELETE_ROWS_NO_SUPPORTED, "ROLLBACK");
        } else {
            // set it to TRUE anyway. too much expects is to be so dispite it never working
            declareTableClauses[2] = Boolean.TRUE;
        }
        return (StatementNode) nodeFactory.getNode(
                C_NodeTypes.CREATE_TABLE_NODE,
                tableName,
                createBehavior,
                tableElementList,
                (Properties)null,
                (Boolean) declareTableClauses[1],
                (Boolean) declareTableClauses[2],
                getContextManager());
    }

    public ValueNode createTruncateTypeNode(ValueNode operandNode, ValueNode truncValue) throws StandardException {
        ValueNode truncateOperand = null;
        if (operandNode == null) {
            throw StandardException.newException(SQLState.LANG_TRUNCATE_NULL_OPERAND);
        } else if (operandNode instanceof UnaryOperatorNode) {
            // date... probably
            String opStr = ((UnaryOperatorNode)operandNode).getOperatorString();
            if ("date".equals(opStr)) {
                truncateOperand = (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE,
                        operandNode,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DATE),
                        getContextManager());
            } else if ("timestamp".equals(opStr)) {
                truncateOperand = (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE,
                        operandNode,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP),
                        getContextManager());
            }
        } else if (operandNode instanceof BinaryOperatorNode) {
            // timestamp... likely
            String opStr = ((BinaryOperatorNode)operandNode).getOperatorString();
            if ("timestamp".equals(opStr)) {
                truncateOperand = (ValueNode) nodeFactory.getNode(
                        C_NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE,
                        operandNode,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP),
                        getContextManager());
            }
        } else if (operandNode instanceof NumericConstantNode) {
            // truncate numberic
            // must have 2 numeric args - 1st is decimal operand, not null; 2nd is integer truncValue, not null

            // default to zero if truncValue null or not numeric
            if (! (truncValue instanceof NumericConstantNode)) {
                truncValue = (NumericConstantNode) nodeFactory.getNode(C_NodeTypes.INT_CONSTANT_NODE,
                        0,  // default to zero
                        getContextManager());
            }

            truncateOperand = operandNode;
        } else if (operandNode instanceof ColumnReference) {
            // truncating a column in a table. could be type date, timestamp or decimal
            // this is enforced in TruncateOperatorNode after binding so that we know all types
            truncateOperand = operandNode;
        } else if (operandNode instanceof CurrentDatetimeOperatorNode) {
            truncateOperand = operandNode;
        }

        if (truncateOperand == null) {
            throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TYPE_OPERAND, operandNode);
        }

        return (ValueNode) nodeFactory.getNode(
                C_NodeTypes.TRUNC_NODE,
                truncateOperand,
                truncValue,
                getContextManager());
    }


    /**
     * Construct a TableElementNode of type
     * C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE.
     *
     * @param defaultNode the new default value node
     * @param columnName  the name of the column to be altered
     * @param autoIncrementInfo autoincrement information collected, if any.
     *
     * @return the new node constructed
     * @exception StandardException standard error policy
     */
    public TableElementNode wrapAlterColumnDefaultValue(
            ValueNode defaultNode,
            String columnName,
            long[] autoIncrementInfo) throws StandardException {

        if (autoIncrementInfo
                [QueryTreeNode.AUTOINCREMENT_IS_AUTOINCREMENT_INDEX] == 0) {
            autoIncrementInfo = null;
        }

        return (TableElementNode) nodeFactory.getNode(
                C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE,
                columnName,
                defaultNode,
                null,
                autoIncrementInfo,
                getContextManager());
    }

    /**
     * Construct a new join node.
     *
     * @param leftRSN the left side of the join
     * @param rightRSN the right side of the join
     * @param onClause the ON clause, or null if there is no ON clause
     * @param usingClause the USING clause, or null if there is no USING clause
     * @param joinType the type of the join (one of the constants INNERJOIN,
     *                 LEFTOUTERJOIN or RIGHTOUTERJOIN or FULLOUTERJOIN in JoinNode)
     * @return a new join node
     */
    public JoinNode newJoinNode(ResultSetNode leftRSN, ResultSetNode rightRSN,
                                 ValueNode onClause, ResultColumnList usingClause,
                                 int joinType)
            throws StandardException
    {
        switch(joinType)
        {
            case JoinNode.INNERJOIN:
                return (JoinNode) nodeFactory.getNode(
                        C_NodeTypes.JOIN_NODE,
                        leftRSN,
                        rightRSN,
                        onClause,
                        usingClause,
                        null,
                        null,
                        null,
                        getContextManager());

            case JoinNode.LEFTOUTERJOIN:
                return (JoinNode) nodeFactory.getNode(
                        C_NodeTypes.HALF_OUTER_JOIN_NODE,
                        leftRSN,
                        rightRSN,
                        onClause,
                        usingClause,
                        Boolean.FALSE,
                        null,
                        getContextManager());

            case JoinNode.RIGHTOUTERJOIN:
                return (JoinNode) nodeFactory.getNode(
                        C_NodeTypes.HALF_OUTER_JOIN_NODE,
                        leftRSN,
                        rightRSN,
                        onClause,
                        usingClause,
                        Boolean.TRUE,
                        null,
                        getContextManager());

            case JoinNode.FULLOUTERJOIN:
                return (JoinNode) nodeFactory.getNode(
                        C_NodeTypes.FULL_OUTER_JOIN_NODE,
                        leftRSN,
                        rightRSN,
                        onClause,
                        usingClause,
                        null,
                        getContextManager());

            default:
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT("Unexpected joinType: " + joinType);
                }
                return null;
        }
    }

    /**
     * Used by create table with data, this method parses the table population query. We pull out what's between
     * the square brackets below []
     * CREATE TABLE X AS [SELECT A, B, C FROM Y] WITH DATA
     */
    static public String parseQueryString(String src) {
        boolean inSection = false;
        StringBuilder buf = new StringBuilder();

        // Use a non-printable character as an end-of-hint marker.
        char endOfHintMarker = 0x01;

        // Strip out C-style comments:   /* This type of comment */
        src = src.replaceAll("(/\\*.*?\\*/)", " ");

        // Strip out comments starting with --, except --splice-properties hints.
        src = src.replaceAll("(--)(?i)(?!splice-properties)(.*?)(\\n|\\r|\\r\\n)", " ");

        // Add an end-of-hint marker to a --splice-properties hint, so we know where to
        // place the newline later on.
        src = src.replaceAll("(--)(?i)(?=splice-properties)(.*?)(\\n|\\r|\\r\\n)",
                "$1$2$3"+Character.toString(endOfHintMarker));

        // If there is a comment before the statement, replacement ops above produce spaces before the
        // CREATE token. The first string after split below would be empty, causing charAt(0) to fail.
        src = src.trim();

        for (String part : src.split("\\s+")) {
            if (part.charAt(0) == endOfHintMarker)
                part = "\n" + part.substring(1);
            if (inSection && part.equalsIgnoreCase("with")) {
                inSection = false;
            } else if (inSection) {
                buf.append(part).append(" ");
            } else if (! inSection && part.equalsIgnoreCase("as")) {
                inSection = true;
            }
        }
        if (buf.length() > 1) {
            // remove the trailing space
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    static public Properties parsePropertyList(String str) throws StandardException {
        Properties properties = new FormatableProperties();
        StringTokenizer commaSeparatedProperties;
        StringTokenizer equalOperatorSeparatedProperty;
        //first use StringTokenizer to get tokens which are delimited by ,s
        commaSeparatedProperties = new StringTokenizer(str,",");
        while (commaSeparatedProperties.hasMoreTokens()) {
            //Now verify that tokens delimited by ,s follow propertyName=value pattern
            String currentProperty = commaSeparatedProperties.nextToken();
            equalOperatorSeparatedProperty = new StringTokenizer(currentProperty, "=", true);
            if (equalOperatorSeparatedProperty.countTokens() != 3)
                throw StandardException.newException(SQLState.PROPERTY_SYNTAX_INVALID);
            else {
                String key = equalOperatorSeparatedProperty.nextToken().trim();
                if (!equalOperatorSeparatedProperty.nextToken().equals("="))
                    throw StandardException.newException(SQLState.PROPERTY_SYNTAX_INVALID);
                String value = equalOperatorSeparatedProperty.nextToken().trim();
                GrammarImpl.verifyImageLength(value);
                /* Trim off the leading and trailing ', and compress all '' to ' */
                if (value.startsWith("'") && value.endsWith("'"))
                    value = StringUtil.compressQuotes(value.substring(1, value.length() - 1), SINGLEQUOTES);
                    /* Trim off the leading and trailing ", and compress all "" to " */
                else if (value.startsWith("\"") && value.endsWith("\""))
                    value = StringUtil.compressQuotes(value.substring(1, value.length() - 1), DOUBLEQUOTES);
                else if (!InsertNode.STATUS_DIRECTORY.equals(key) &&
                        !InsertNode.BULK_IMPORT_DIRECTORY.equals(key) &&
                        !DeleteNode.BULK_DELETE_DIRECTORY.equals(key))
                    value = value.toUpperCase();
                // Do not allow user to specify multiple values for the same key
                if (properties.put(key, value) != null) {
                    throw StandardException.newException(SQLState.LANG_DUPLICATE_PROPERTY, key);
                }
            }
        }
        return properties;
    }

    static public int getLength(String s) throws StandardException {
        try
        {
            char modifier = s.charAt(s.length()-1);
            String number = s.substring(0, s.length()-1); // in case of ending w. letter
            long mul;
            switch (modifier) {
                case 'G':
                case 'g':
                    mul =1073741824L;    //1 Giga
                    break;
                case 'M':
                case 'm':
                    mul=1048576L;        // 1 Mega
                    break;
                case 'K':
                case 'k':
                    mul=1024L;        // 1 Kilo
                    break;
                default:
                    mul=1;
                    number = s; // no letter in end, need whole string
                    break;
            }
            long    specifiedLength = Long.parseLong(number) * mul;

            // match DB2 limits of 1 to 2147483647
            if ((specifiedLength > 0L) &&
                    (specifiedLength <= Limits.DB2_LOB_MAXWIDTH))
            {
                return (int)specifiedLength;
            }

            // DB2 allows 2G or 2048M or 2097152k that calculate out to
            // 2147483648, but sets the length to be one less.
            if (mul != 1 && specifiedLength == 2147483648L)
                return Limits.DB2_LOB_MAXWIDTH;

        }
        catch (NumberFormatException nfe)
        {
        }

        throw StandardException.newException(
                SQLState.LANG_INVALID_COLUMN_LENGTH, s);
    }
}