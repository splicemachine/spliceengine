package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

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
}