package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;
import org.python.antlr.op.Param;

import java.sql.Types;
import java.util.Properties;
import java.util.Vector;

import static com.splicemachine.db.impl.sql.compile.SQLParserConstants.*;

class SQLGrammarImpl {
    static final String[] SAVEPOINT_CLAUSE_NAMES = {"UNIQUE", "ON ROLLBACK RETAIN LOCKS", "ON ROLLBACK RETAIN CURSORS"};

    /* Keep in synch with CreateAliasNode's index constants */
    static final String[] ROUTINE_CLAUSE_NAMES =
            {null,
                    "SPECIFIC",
                    "RESULT SET",
                    "LANGUAGE",
                    "EXTERNAL NAME",
                    "PARAMETER STYLE",
                    "SQL",
                    "DETERMINISTIC",
                    "ON NULL INPUT",
                    "RETURN TYPE",
                    "EXTERNAL SECURITY"};
    /**
     Clauses required for Java routines. Numbers correspond
     to offsets in ROUTINE_CLAUSE_NAMES.
     3 - "LANGUAGE"
     4 - "EXTERNAL NAME"
     5 - "PARAMETER STYLE"
     */
    static final int[] JAVA_ROUTINE_CLAUSES = {CreateAliasNode.LANGUAGE,CreateAliasNode.EXTERNAL_NAME,CreateAliasNode.PARAMETER_STYLE};
    static final String[] TEMPORARY_TABLE_CLAUSE_NAMES = {"NOT LOGGED", "ON COMMIT", "ON ROLLBACK"};
    /* The default length of a char or bit if the length is omitted */
    static final int    DEFAULT_STRING_COLUMN_LENGTH = 1;

    // Defines for ON or USING clauses
    static final int    ON_OR_USING_CLAUSE_SIZE = 2;
    static final int    ON_CLAUSE = 0;
    static final int    USING_CLAUSE = 1;

    // Defines for optional table clauses
    static final int    OPTIONAL_TABLE_CLAUSES_SIZE = 3;
    static final int    OPTIONAL_TABLE_CLAUSES_TABLE_PROPERTIES = 0;
    static final int    OPTIONAL_TABLE_CLAUSES_DERIVED_RCL = 1;
    static final int    OPTIONAL_TABLE_CLAUSES_CORRELATION_NAME = 2;

    // Define for UTF8 max
    static final int    MAX_UTF8_LENGTH = 65535;

    // Constants for set operator types
    static final int NO_SET_OP = 0;
    static final int UNION_OP = 1;
    static final int UNION_ALL_OP = 2;
    static final int EXCEPT_OP = 3;
    static final int EXCEPT_ALL_OP = 4;
    static final int INTERSECT_OP = 5;
    static final int INTERSECT_ALL_OP = 6;

    // indexes into array of optional clauses for CREATE SEQUENCE statement
    static final int IDX_DATA_TYPE = 0;
    static final int IDX_START_WITH_OPTION = IDX_DATA_TYPE + 1;
    static final int IDX_INCREMENT_BY_OPTION = IDX_START_WITH_OPTION + 1;
    static final int IDX_MAX_VALUE_OPTION = IDX_INCREMENT_BY_OPTION + 1;
    static final int IDX_MIN_VALUE_OPTION = IDX_MAX_VALUE_OPTION + 1;
    static final int IDX_CYCLE_OPTION = IDX_MIN_VALUE_OPTION + 1;
    static final int IDX_OPTION_COUNT = IDX_CYCLE_OPTION + 1;

    // indexes into array of clauses for OFFSET FETCH NEXT
    static final int OFFSET_CLAUSE = 0;
    static final int FETCH_FIRST_CLAUSE = OFFSET_CLAUSE + 1;
    static final int OFFSET_CLAUSE_COUNT = FETCH_FIRST_CLAUSE + 1;

    Object[]                    paramDefaults;
    String                        statementSQLText;

    /* The number of the next ? parameter */
    int            parameterNumber;

    /* The list of ? parameters */
    Vector            parameterList;

    /* Remember if the last identifier or keyword was a
     * delimited identifier.  This is used for remembering
     * if the xxx in SERIALIZE(xxx) was a delimited identifier
     * because we need to know whether or not we can convert
     * xxx to upper case if we try to resolve it as a class
     * alias at bind time.
     */
    Boolean lastTokenDelimitedIdentifier = Boolean.FALSE;
    Boolean    nextToLastTokenDelimitedIdentifier = Boolean.FALSE;


    /*
     ** Remember the last token we got that was an identifier
     */
    Token    lastIdentifierToken;
    Token    nextToLastIdentifierToken;

    static final String SINGLEQUOTES = "\'\'";
    static final String DOUBLEQUOTES = "\"\"";

    static final String DEFAULT_INDEX_TYPE = "BTREE";

    //the following 2 booleans are used to make sure only null or not null is
    //defined for a column while creating a table or altering a table. Defining
    //both at the same time will be an error case.
    boolean explicitNotNull = false;
    boolean explicitNull = false;

    //this vector keeps a list of explicitly nullable columns, so that if they
    //get used in the table level primary key constraint, it will result in an
    //exception.
    Vector explicitlyNullableColumnsList = new Vector();

    public NodeFactory nodeFactory;
    public ContextManager cm;
    public CompilerContext compilerContext;

    // Get the current ContextManager
    public final ContextManager getContextManager()
    {
        return cm;
    }


    CompilerContext getCompilerContext() {
        return compilerContext;
    }

    /**
     * Get the nodeFactory for this database.
     *
     * @return    The nodeFactory for this database.
     * @exception StandardException        Thrown on error
     */
     NodeFactory getNodeFactory() throws StandardException
    {
        if ( nodeFactory == null )
        {
            nodeFactory = getCompilerContext().getNodeFactory();
        }

        return nodeFactory;
    }
    final void setCompilerContext(CompilerContext cc) {
        compilerContext = cc;
        cm = cc.getContextManager();
    }


    DataTypeDescriptor getDataTypeServices(int type, int precision, int scale,
                                                   int length)
            throws StandardException
    {
        return new DataTypeDescriptor(
                TypeId.getBuiltInTypeId(type),
                precision,
                scale,
                true, /* assume nullable for now, change it if not nullable */
                length
        );
    }

    DataTypeDescriptor getJavaClassDataTypeDescriptor(TableName typeName)
            throws StandardException
    {
        return new DataTypeDescriptor
                (
                        TypeId.getUserDefinedTypeId( typeName.getSchemaName(), typeName.getTableName(), null ),
                        true
                );
    }
    LanguageConnectionContext getLanguageConnectionContext()
    {
        return (LanguageConnectionContext) getContextManager().getContext(
                LanguageConnectionContext.CONTEXT_ID);
    }

    /**
     Utility method for checking that the underlying database has been
     upgraded to the required level to use this functionality. Used to
     disallow SQL statements that would leave on-disk formats that would
     not be understood by a engine that matches the current upgrade level
     of the database. Throws an exception if the database is not a the required level.
     <P>
     Typically used for CREATE statements at the parser level. Called usually just
     before the node is created, or can be called in just a partial syntax fragment

     @param version Data Dictionary major version (DataDictionary.DD_ constant)
     @param feature SQL Feature name, for error text.
     */
    boolean checkVersion(int version, String feature) throws StandardException
    {
        return getLanguageConnectionContext()
                .getDataDictionary().checkVersion(version, feature);
    }

    /**
     Utility method for checking that the underlying database uses SQL standard
     permission checking (GRANT/REVOKE).

     @param command "GRANT", "REVOKE", "CREATE/DROP/SET ROLE" or CURRENT_ROLE
     */
    void checkSqlStandardAccess(String command) throws StandardException
    {
        if( getLanguageConnectionContext().usesSqlAuthorization())
            return;

        throw StandardException.newException(SQLState.LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS,
                command,
                Property.SQL_AUTHORIZATION_PROPERTY,
                "TRUE");
    }

    /**
     Prevent NEXT VALUE FOR clauses when we get to the bind() phase.
     */
    void forbidNextValueFor()
    {
        CompilerContext cc = getCompilerContext();

        cc.setReliability( cc.getReliability() | CompilerContext.NEXT_VALUE_FOR_ILLEGAL );
    }

    /**
     * check if the type length is ok for the given type.
     */
    void checkTypeLimits(int type, int length)
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

    /**
     Check that the current mode supports internal extensions.

     @param feature Description of feature for exception.

     @exception StandardException current mode does not support statement
     */
    void checkInternalFeature(String feature) throws StandardException
    {
        //CompilerContext cc = getCompilerContext();
        //if ((cc.getReliability() & CompilerContext.INTERNAL_SQL_ILLEGAL) != 0)
        //  throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, feature);
    }


    static void verifyImageLength(String image) throws StandardException
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
    static String normalizeDelimitedID(String str)
    {
        str = StringUtil.compressQuotes(str, DOUBLEQUOTES);
        return str;
    }
    static boolean isDATETIME(int val)
    {
        if (val == DATE || val == TIME || val == TIMESTAMP)
            return true;
        else
            return false;
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

    ValueNode multOp(ValueNode leftOperand,
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
                return new BinaryArithmeticOperatorNode(C_NodeTypes.BINARY_TIMES_OPERATOR_NODE,
                        leftOperand, rightOperand, getContextManager());

            case BinaryOperatorNode.DIVIDE:
                return new BinaryArithmeticOperatorNode(C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE,
                        leftOperand, rightOperand, getContextManager());

            case BinaryOperatorNode.CONCATENATE:
                return new ConcatenationOperatorNode(
                        leftOperand, rightOperand, getContextManager());

            default:
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Unexpected multiplicative operator " +
                            multiplicativeOperator);
                return null;
        }
    }

    /**
     * Set up and like the parameters to the descriptors.
     * Set all the ParameterNodes to point to the array of
     * parameter descriptors.
     *
     *    @exception    StandardException
     */
    void setUpAndLinkParameters()
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
     *  Initializes the list of unnamed parameters, i.e., "?" parameters
     *
     *    Usually, this routine just gets an empty list for the unnamed parameters.
     *
     *
     */
    void    initUnnamedParameterList()
    {
        parameterList = new Vector();
    }

    /**
     * Makes a new unnamed ParameterNode and chains it onto parameterList.
     *
     *    @return    new unnamed parameter.
     *
     *    @exception    StandardException
     */
    ParameterNode    makeParameterNode(  )
            throws StandardException
    {
        ParameterNode    parm;
        DataValueDescriptor sdv = null;

        if ((paramDefaults != null) && (parameterNumber < paramDefaults.length))
        {
            sdv = (DataValueDescriptor) paramDefaults[parameterNumber];
        }
        parm = new ParameterNode(getContextManager(),
                ReuseFactory.getInteger(parameterNumber),
                sdv);

        parameterNumber++;
        parameterList.addElement(parm);

        return parm;
    }

    /**
     * Looks up an unnamed parameter given its parameter number.
     *
     *    @param    paramNumber        Number of parameter in unnamed
     *                            parameter list.
     *
     *    @return    corresponding unnamed parameter.
     *
     */
    ParameterNode    lookupUnnamedParameter( int paramNumber )
    {
        ParameterNode        unnamedParameter;

        unnamedParameter = (ParameterNode) parameterList.elementAt( paramNumber );
        return unnamedParameter;
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
            return new NumericConstantNode(cm,
                    C_NodeTypes.INT_CONSTANT_NODE,
                    Integer.valueOf(num));
        }
        catch (NumberFormatException nfe)
        {
            // we catch because we want to continue on below
        }

        // next, see if it might be a long
        try
        {
            return new NumericConstantNode(cm,
                    C_NodeTypes.LONGINT_CONSTANT_NODE,
                    Long.valueOf(num));
        }
        catch (NumberFormatException nfe)
        {
            if (intsOnly) {
                throw nfe;
            }
            // else we want to continue on below
        }

        NumericConstantNode ncn =
            new NumericConstantNode(cm, C_NodeTypes.DECIMAL_CONSTANT_NODE, num);
        if (ncn != null) {
            int precision = ncn.getTypeServices().getPrecision();
            if (precision > TypeCompiler.MAX_DECIMAL_PRECISION_SCALE)
                throw StandardException.newException(SQLState.DECIMAL_TOO_MANY_DIGITS);
        }
        return ncn;

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
        SelectNode resultSet = new SelectNode(
                null,
                null,     /* AGGREGATE list */
                fromList, /* FROM list */
                whereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager());

        DeleteNode dn = new DeleteNode(getContextManager(), tableName, resultSet, targetProperties);
        setUpAndLinkParameters();
        return dn;
    }

    /**
     * Get an UPDATE node given the pieces.
     *
     *
     *    @exception    StandardException
     */
    StatementNode getUpdateNode(FromTable fromTable,
                                        TableName tableName,
                                        ResultColumnList setClause,
                                        ValueNode whereClause)
            throws StandardException
    {
        FromList   fromList = (FromList) nodeFactory.getNode(
                C_NodeTypes.FROM_LIST,
                getContextManager());

        fromList.addFromTable(fromTable);

        SelectNode resultSet = new SelectNode(
                setClause, /* SELECT list */
                null,     /* AGGREGATE list */
                fromList, /* FROM list */
                whereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager() );

        UpdateNode node = new UpdateNode(
                tableName, /* target table for update */
                resultSet, /* SelectNode just created */
                getContextManager());
        setUpAndLinkParameters();

        return node;
    }

    StatementNode getUpdateNodeWithSub(FromTable fromTable, /* table to be updated */
                                               TableName tableName, /* table to be updated */
                                               ResultColumnList setClause, /* new values to ue for the update */
                                               ValueNode whereClause, /* where clause for outer update */
                                               ValueNode subQuery) /* inner source subquery for multi column syntax */
            throws StandardException
    {
        FromList fromList = (FromList) nodeFactory.getNode(
                C_NodeTypes.FROM_LIST,
                getContextManager());
        fromList.addFromTable(fromTable);

        // Don't flatten the subquery here but build it as a derived table. If we
        // want to flatten the subquery here, it has to be fully bound in order
        // to cover many corner cases. That is not an easy task and would lead to
        // even more hacky code.
        // The derived table flattening logic in preprocess will be triggered to
        // flatten the subquery.
        SubqueryNode subq = (SubqueryNode) subQuery;
        FromTable fromSubq = (FromTable) nodeFactory.getNode(
                C_NodeTypes.FROM_SUBQUERY,
                subq.getResultSet(),
                subq.getOrderByList(),
                subq.getOffset(),
                subq.getFetchFirst(),
                subq.hasJDBClimitClause(),
                UpdateNode.SUBQ_NAME,
                null,  /* derived RCL */
                null,  /* optional table clause */
                getContextManager());
        fromList.addFromTable(fromSubq);

        SelectNode innerSelect = (SelectNode) subq.getResultSet();
        // Reject select star in subQuery. Otherwise, we need to bind subQuery to
        // expand columns. Unfortunately, it is not easy at this time.
        innerSelect.verifySelectStarSubquery(fromList, subq.getSubqueryType());

        // A derived table cannot reference columns from tables that are in front of
        // the derived table in the same from list. But correlation must be allowed
        // in subquery for update statement. For this reason, pull where clause up
        // early so that binding can be done successfully.
        //
        // Inner where clause may contain column references that uses table aliases
        // defined inside the subquery. They need to be updated but it's not possible
        // until at least tables are bound. For this reason, it's done in
        // UpdateNode's bindStatement().
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
        innerSelect.setWhereClause(null);

        // Build column references as expressions to the results columns generated
        // for set clause. If a result column is assigned with an expression,
        // generate a name for that expression and still build column reference. It
        // has to be this way because we generate an alias for the derived table.
        // Once that's done, we wouldn't be able to access columns using an table
        // aliases within the derived table.
        ResultColumnList innerRCL = innerSelect.getResultColumns();
        int inputSize = setClause.size();
        if (inputSize != innerRCL.size()) {
            throw StandardException.newException(SQLState.LANG_UNION_UNMATCHED_COLUMNS, "assignment in set clause");
        }
        for (int index = 0; index < inputSize; index++)
        {
            ResultColumn rc = setClause.elementAt(index);
            ResultColumn innerResultColumn = innerRCL.elementAt(index);
            String innerColumnName = innerResultColumn.getName();
            if (innerColumnName == null) {
                innerColumnName = "UPD_SUBQ_COL_" + index;
                innerResultColumn.setName(innerColumnName);
                innerResultColumn.setNameGenerated(true);
            }
            ValueNode colRef = (ValueNode) getNodeFactory().getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    innerColumnName,
                    ((FromTable)fromList.getNodes().get(1)).getTableName(),
                    getContextManager());
            rc.setExpression(colRef);
        }

        SelectNode resultSet = new SelectNode(
                setClause, /* SELECT list */
                null,   /* AGGREGATE list */
                fromList, /* FROM list */
                alteredWhereClause, /* WHERE clause */
                null, /* GROUP BY list */
                null, /* having clause */
                null, /* window list */
                getContextManager() );

        UpdateNode node = new UpdateNode(
                        tableName, /* target table for update */
                        resultSet, /* SelectNode just created */
                        getContextManager()
                    );
        node.setUpdateWithSubquery(true);

        setUpAndLinkParameters();

        return node;
    }

    /**
     * Generate a trim operator node
     * @param trimSpec one of Leading, Trailing or Both.
     * @param trimChar the character to trim. Can be null in which case it defaults
     * to ' '.
     * @param trimSource expression to be trimmed.
     */
    ValueNode getTrimOperatorNode(Integer trimSpec, ValueNode trimChar,
                                          ValueNode trimSource, ContextManager cm) throws StandardException
    {
        if (trimChar == null)
        {
            trimChar = (CharConstantNode) nodeFactory.getNode(
                    C_NodeTypes.CHAR_CONSTANT_NODE,
                    " ",
                    getContextManager());
        }
        return new TernaryOperatorNode(
                C_NodeTypes.TRIM_OPERATOR_NODE,
                trimSource, // receiver
                trimChar,   // leftOperand.
                null, // right
                ReuseFactory.getInteger(TernaryOperatorNode.TRIM),
                trimSpec,
                cm == null ? getContextManager() : cm);
    }

    /**
     List of JDBC escape functions that map directly onto
     a function in the SYSFUN schema.
     */
    static final String[] ESCAPED_SYSFUN_FUNCTIONS =
            {"ACOS", "ASIN", "ATAN", "ATAN2", "COS", "SIN", "TAN", "PI",
                    "DEGREES", "RADIANS", "EXP", "LOG", "LOG10", "CEILING", "FLOOR",
                    "SIGN", "RAND", "ROUND", "COT" };

    /**
     Convert a JDBC escaped function name to a function
     name in the SYSFUN schema. Returns null if no such
     function exists.
     */
    String getEscapedSYSFUN(String name)
    {
        name = StringUtil.SQLToUpperCase(name);

        for (int i = 0; i < ESCAPED_SYSFUN_FUNCTIONS.length; i++)
        {
            if (ESCAPED_SYSFUN_FUNCTIONS[i].equals(name))
                return name;
        }
        return null;
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
    StatementNode
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
                Character.valueOf(aliasType),
                delimitedIdentifier,
                cm );
    }

    /**
     Create a node for the drop alias/procedure call.
     */
    StatementNode dropAliasNode(Object aliasName, char type) throws StandardException
    {
        return new DropAliasNode(aliasName, Character.valueOf(type), getContextManager());
    }

    /**
     * Get a substring node from
     *      - the string
     *      - the start position
     *      - the length
     *      - a boolean values for specifying the kind of substring function
     * @exception StandardException  Thrown on error
     */
    ValueNode getSubstringNode( ValueNode stringValue, ValueNode startPosition,
                                ValueNode length, Boolean boolVal ) throws StandardException
    {
        return new TernaryOperatorNode(
                C_NodeTypes.SUBSTRING_OPERATOR_NODE,
                stringValue,
                startPosition,
                length,
                ReuseFactory.getInteger(TernaryOperatorNode.SUBSTRING),
                null,
                getContextManager());
    }

    ValueNode getRightOperatorNode(ValueNode stringValue, ValueNode length) throws StandardException
    {
        return new TernaryOperatorNode(
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
    ValueNode getLeftOperatorNode(ValueNode stringValue, ValueNode length) throws StandardException
    {
        return new TernaryOperatorNode(
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
     * @param fromString from sub string
     * @param toString to string
     * @exception StandardException thrown on error
     */
    ValueNode getReplaceNode(
            ValueNode stringValue,
            ValueNode fromString,
            ValueNode toString) throws StandardException
    {
        return new TernaryOperatorNode(
                C_NodeTypes.REPLACE_OPERATOR_NODE,
                stringValue,
                fromString,
                toString,
                ReuseFactory.getInteger(TernaryOperatorNode.REPLACE),
                null,
                getContextManager());
    }

    void initStatement( String statementSQLText, Object[] paramDefaults)
            throws StandardException
    {
        /* Do per-statement initialization here */
        parameterNumber = 0;
        this.statementSQLText = statementSQLText;
        this.paramDefaults = paramDefaults;
        getNodeFactory();
        initUnnamedParameterList();
    } // End of initStatement

    ValueNode getJdbcIntervalNode( int intervalType) throws StandardException
    {
        return (ValueNode) nodeFactory.getNode( C_NodeTypes.INT_CONSTANT_NODE,
                ReuseFactory.getInteger( intervalType),
                getContextManager());
    }

    /**
     Check to see if the required claues have been added
     to a procedure or function defintion.

     @param required int array of require clauses
     @param  clauses the array of declared clauses.
     */
    void checkRequiredRoutineClause(int[] required, Object[] clauses)
            throws StandardException
    {
        for (int i = 0; i < required.length; i++)
        {
            int re = required[i];
            if (clauses[re] == null)
            {
                throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
                        ROUTINE_CLAUSE_NAMES[re]);
            }
        }
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
    TableElementNode wrapAlterColumnDefaultValue(
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

    boolean
    isTableValueConstructor(ResultSetNode expression)
            throws StandardException {

        return expression instanceof RowResultSetNode ||
                (expression instanceof UnionNode &&
                        ((UnionNode)expression).tableConstructor());
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
    JoinNode newJoinNode(ResultSetNode leftRSN, ResultSetNode rightRSN,
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

    void setWindowFrameMode(QueryTreeNode node, WindowFrameDefinition.FrameMode mode) {
        WindowDefinitionNode wdn = (WindowDefinitionNode) node;
        WindowFrameDefinition frame = wdn.getFrameExtent();
        frame.setFrameMode(mode);

    }

    /**
     * For temporary table validation and creation.
     */
    StatementNode verifySyntaxAndCreate(Object[] declareTableClauses,
                                        Boolean isGlobal,
                                        TableName tableName,
                                        TableElementList tableElementList) throws StandardException {
        // NOT LOGGED is allways true
        // if ON COMMIT behavior not explicitly specified in DECLARE command, default to ON COMMIT PRESERVE ROWS
        assert declareTableClauses.length == 3;
        if (declareTableClauses[1] == null) {
            declareTableClauses[1] = Boolean.FALSE;
        } else if (declareTableClauses[1].equals(Boolean.TRUE) ) {
            // ON COMMIT DELETE ROWS is not supported
            throw StandardException.newException(SQLState.LANG_TEMP_TABLE_DELETE_ROWS_NO_SUPPORTED, "COMMIT");
        }
        // if ON ROLLBACK behavior not explicitly specified in DECLARE command, default to ON ROLLBACK DELETE ROWS
        if (declareTableClauses[2] != null && declareTableClauses[2].equals(Boolean.TRUE) ) {
            // ON ROLLBACK DELETE ROWS is not supported
            throw StandardException.newException(SQLState.LANG_TEMP_TABLE_DELETE_ROWS_NO_SUPPORTED, "ROLLBACK");
        } else {
            // set it to TRUE anyway. too much expects is to be so dispite it never working
            declareTableClauses[2] = Boolean.TRUE;
        }
        return (StatementNode) nodeFactory.getNode(
                C_NodeTypes.CREATE_TABLE_NODE,
                tableName,
                tableElementList,
                (Properties)null,
                (Boolean) declareTableClauses[1],
                (Boolean) declareTableClauses[2],
                getContextManager());
    }

    ValueNode createTruncateTypeNode(ValueNode operandNode, ValueNode truncValue) throws StandardException {
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
                truncValue = new NumericConstantNode(cm, C_NodeTypes.INT_CONSTANT_NODE,
                        0  // default to zero
                        );
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
     * Used by create table with data, this method parses the table population query. We pull out what's between
     * the square brackets below []
     * CREATE TABLE X AS [SELECT A, B, C FROM Y] WITH DATA
     */
    public String parseQueryString(String src) {
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
}