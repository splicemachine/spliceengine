package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.sql.Types;
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
     * Set up and like the parameters to the descriptors.
     * Set all the ParameterNodes to point to the array of
     * parameter descriptors.
     *
     *    @exception    StandardException
     */
    void setUpAndLinkParameters(Vector parameterList)
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
}