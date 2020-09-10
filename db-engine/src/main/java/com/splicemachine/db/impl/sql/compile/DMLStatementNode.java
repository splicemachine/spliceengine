/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.sql.compile.subquery.SubqueryFlattening;

import java.util.Collection;
import java.util.Vector;

/**
 * A DMLStatementNode represents any type of DML statement: a cursor declaration, an INSERT statement, and UPDATE
 * statement, or a DELETE statement.  All DML statements have result sets, but they do different things with them.  A
 * SELECT statement sends its result set to the client, an INSERT statement inserts its result set into a table, a
 * DELETE statement deletes from a table the rows corresponding to the rows in its result set, and an UPDATE statement
 * updates the rows in a base table corresponding to the rows in its result set.
 */

public abstract class DMLStatementNode extends StatementNode {

    /**
     * The result set is the rows that result from running the statement.  What this means for SELECT statements is
     * fairly obvious. For a DELETE, there is one result column representing the key of the row to be deleted (most
     * likely, the location of the row in the underlying heap).  For an UPDATE, the row consists of the key of the row
     * to be updated plus the updated columns.  For an INSERT, the row consists of the new column values to be inserted,
     * with no key (the system generates a key). <p/> The parser doesn't know anything about keys, so the columns
     * representing the keys will be added after parsing (perhaps in the binding phase?).
     */
    ResultSetNode resultSet;

    /**
     * Initializer for a DMLStatementNode
     *
     * @param resultSet A ResultSetNode for the result set of the DML statement
     */

    public void init(Object resultSet) {
        this.resultSet = (ResultSetNode) resultSet;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);
            if (resultSet != null) {
                printLabel(depth, "resultSet: ");
                resultSet.treePrint(depth + 1);
            }
        }
    }

    /**
     * Get the ResultSetNode from this DML Statement. (Useful for view resolution after parsing the view definition.)
     *
     * @return ResultSetNode    The ResultSetNode from this DMLStatementNode.
     */
    public ResultSetNode getResultSetNode() {
        return resultSet;
    }

    /**
     * Bind only the underlying ResultSets with tables.  This is necessary for INSERT, where the binding order depends
     * on the underlying ResultSets. This means looking up tables and columns and getting their types, and figuring out
     * the result types of all expressions, as well as doing view resolution, permissions checking, etc.
     *
     * @param dataDictionary The DataDictionary to use to look up columns, tables, etc.
     * @return The bound query tree
     * @throws StandardException Thrown on error
     */

    public QueryTreeNode bindResultSetsWithTables(DataDictionary dataDictionary)
            throws StandardException {
        /* Okay to bindly bind the tables, since ResultSets without tables
         * know to handle the call.
         */
        bindTables(dataDictionary);

        /* Bind the expressions in the underlying ResultSets with tables */
        bindExpressionsWithTables();

        return this;
    }

    /**
     * Optimize a DML statement (which is the only type of statement that should need optimizing, I think). This method
     * over-rides the one in QueryTreeNode. <p/> This method takes a bound tree, and returns an optimized tree. It
     * annotates the bound tree rather than creating an entirely new tree. <p/> Throws an exception if the tree is not
     * bound, or if the binding is out of date.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void optimizeStatement() throws StandardException {

        //prune projection list
        if (getCompilerContext().isProjectionPruningEnabled())
            accept(new ProjectionPruningVisitor());

        /* Perform subquery flattening if applicable. */
        SubqueryFlattening.flatten(this);
        /* it is possible that some where clause subquery will be converted to fromSubquery in preprocess(),
           so we need to compute the maximum possible number of tables that take into consideration
           of the where subqueries
         */
        CountWhereSubqueryVisitor countWhereSubqueryVisitor = new CountWhereSubqueryVisitor();
        accept(countWhereSubqueryVisitor);
        int numOfWhereSubqueries = countWhereSubqueryVisitor.getCount();

        int maximalPossibleTableCount = getCompilerContext().getNumTables() + numOfWhereSubqueries;
        getCompilerContext().setMaximalPossibleTableCount(maximalPossibleTableCount);

        resultSet = resultSet.preprocess(maximalPossibleTableCount, null, null);
        // Evaluate expressions with constant operands here to simplify the
        // query tree and to reduce the runtime cost. Do it before optimize()
        // since the simpler tree may have more accurate information for
        // the optimizer. (Example: The selectivity for 1=1 is estimated to
        // 0.1, whereas the actual selectivity is 1.0. In this step, 1=1 will
        // be rewritten to TRUE, which is known by the optimizer to have
        // selectivity 1.0.)
        // SelectNodes have already been visited in SelectNode.java,
        // so handle all other nodes here we might have missed,
        // skipping over SelectNodes in the tree traversal.
        accept(new ConstantExpressionVisitor(SelectNode.class));

        // prune tree based on unsat condition
        accept(new TreePruningVisitor());



        // We should try to cost control first, and fallback to spark if necessary
        DataSetProcessorType connectionType = getLanguageConnectionContext().getDataSetProcessorType();
        getCompilerContext().setDataSetProcessorType(connectionType);

        if (shouldRunControl(resultSet)) {
            resultSet = resultSet.optimize(getDataDictionary(), null, 1.0d, false);
        }

        if (shouldRunSpark(resultSet)) {
            CollectNodesVisitor cnv = new CollectNodesVisitor(FromTable.class);
            resultSet.accept(cnv);
            for (Object obj : cnv.getList()) {
                FromTable ft = (FromTable) obj;
                ft.resetAccessPaths();
                if (ft instanceof JoinNode)
                    ((JoinNode)ft).resetOptimized();
            }
            resultSet = resultSet.optimize(getDataDictionary(), null, 1.0d, true);
        }

        resultSet = resultSet.modifyAccessPaths();

    }

    private boolean shouldRunControl(ResultSetNode resultSet) throws StandardException {
        DataSetProcessorType type = getCompilerContext().getDataSetProcessorType();
        if (type.isForced()) {
            return (!type.isSpark());
        }
        CollectNodesVisitor cnv = new CollectNodesVisitor(FromTable.class);
        resultSet.accept(cnv);
        for (Object obj : cnv.getList()) {
            FromTable ft = (FromTable) obj;
            ft.verifyProperties(getDataDictionary());
            type = type.combine(ft.getDataSetProcessorType());
        }
        getCompilerContext().setDataSetProcessorType(type);
        return !type.isSpark();
    }

    private boolean shouldRunSpark(ResultSetNode resultSet) throws StandardException {
        DataSetProcessorType type = getCompilerContext().getDataSetProcessorType();
        if (!type.isSpark() && (type.isHinted() || type.isForced())) {
            return false;
        }
        CollectNodesVisitor cnv = new CollectNodesVisitor(FromTable.class);
        resultSet.accept(cnv);
        for (Object obj : cnv.getList()) {
            if (obj instanceof FromBaseTable) {
                ((FromBaseTable) obj).determineSpark();
            }
            type = type.combine(((FromTable) obj).getDataSetProcessorType());
        }
        getCompilerContext().setDataSetProcessorType(type);
        return type.isSpark();
    }

    /**
     * Make a ResultDescription for use in a PreparedStatement. <p/> ResultDescriptions are visible to JDBC only for
     * cursor statements. For other types of statements, they are only used internally to get descriptions of the base
     * tables being affected.  For example, for an INSERT statement, the ResultDescription describes the rows in the
     * table being inserted into, which is useful when the values being inserted are of a different type or length than
     * the columns in the base table.
     *
     * @return A ResultDescription for this DML statement
     */

    public ResultDescription makeResultDescription() {
        ResultColumnDescriptor[] colDescs = resultSet.makeResultDescriptors();
        String statementType = statementToString();

        return getExecutionFactory().getResultDescription(
                colDescs, statementType);
    }

    /**
     * A read statement is atomic (DMLMod overrides us) if there are no work units, and no SELECT nodes, or if its
     * SELECT nodes are all arguments to a function.  This is admittedly
     * a bit simplistic, what if someone has: <pre>
     *     VALUES myfunc(SELECT max(c.commitFunc()) FROM T)
     * </pre>
     * but we aren't going too far out of our way to catch every possible wierd case.  We basically want to be
     * permissive w/o allowing someone to partially commit a write.
     *
     * @return true if the statement is atomic
     * @throws StandardException on error
     */
    public boolean isAtomic() throws StandardException {
        /*
        ** If we have a FromBaseTable then we have
        ** a SELECT, so we want to consider ourselves
        ** atomic.  Don't drill below StaticMethodCallNodes
        ** to allow a SELECT in an argument to a method
        ** call that can be atomic.
        */
        HasNodeVisitor visitor = new HasNodeVisitor(FromBaseTable.class, StaticMethodCallNode.class);

        this.accept(visitor, null);
        return visitor.hasNode();
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (resultSet != null) {
            resultSet = (ResultSetNode) resultSet.accept(v, this);
        }
    }

    /**
     * Bind the tables in this DML statement.
     *
     * @param dataDictionary The data dictionary to use to look up the tables
     * @throws StandardException Thrown on error
     */

    protected void bindTables(DataDictionary dataDictionary) throws StandardException {
        /* Bind the tables in the resultSet
         * (DMLStatementNode is above all ResultSetNodes, so table numbering
         * will begin at 0.)
         * In case of referential action on delete , the table numbers can be
         * > 0 because the nodes are create for dependent tables also in the
         * the same context.
         */

        resultSet = resultSet.bindNonVTITables(
                dataDictionary,
                (FromList) getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()));
        resultSet = resultSet.bindVTITables(
                (FromList) getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()));
    }

    protected void bindExpressions(boolean bindResultSet) throws StandardException {
        FromList fromList = (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());

        /* Bind the expressions under the resultSet */
        if (bindResultSet) {
            resultSet.bindExpressions(fromList);
        }

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(fromList.isEmpty(),
                    "fromList.size() is expected to be 0, not " + fromList.size() +
                            " on return from RS.bindExpressions()");
    }

    /**
     * Bind the expressions in this DML statement.
     *
     * @throws StandardException Thrown on error
     */

    protected void bindExpressions() throws StandardException {
        FromList fromList = (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());

        /* Bind the expressions under the resultSet */
        resultSet.bindExpressions(fromList);

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(fromList.isEmpty(),
                    "fromList.size() is expected to be 0, not " + fromList.size() +
                            " on return from RS.bindExpressions()");
    }

    protected void bindTargetExpressions() throws StandardException {
        FromList fromList = (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());

        /* Bind the expressions under the resultSet */
        resultSet.bindTargetExpressions(fromList, false);

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(fromList.size() == 0,
                    "fromList.size() is expected to be 0, not " + fromList.size() +
                            " on return from RS.bindTargetExpressions()");
    }
    /**
     * Bind the expressions in the underlying ResultSets with tables.
     *
     * @throws StandardException Thrown on error
     */
    protected void bindExpressionsWithTables() throws StandardException {
        FromList fromList = (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());

        /* Bind the expressions under the resultSet */
        resultSet.bindExpressionsWithTables(fromList);

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(fromList.isEmpty(),
                    "fromList.size() is expected to be 0, not " + fromList.size() +
                            " on return from RS.bindExpressions()");
    }

    /**
     * Bind this DMLStatementNode.  This means looking up tables and columns and getting their types, and figuring out
     * the result types of all expressions, as well as doing view resolution, permissions checking, etc.
     *
     * @param dataDictionary The DataDictionary to use to look up columns, tables, etc.
     * @return The bound query tree
     * @throws StandardException Thrown on error
     */

    QueryTreeNode bind(DataDictionary dataDictionary) throws StandardException {
        // We just need select privilege on most columns and tables
        getCompilerContext().pushCurrentPrivType(getPrivType());
        try {
            /*
            ** Bind the tables before binding the expressions, so we can
            ** use the results of table binding to look up columns.
            */
            bindTables(dataDictionary);

            /* Bind the expressions */
            bindExpressions();
        } finally {
            getCompilerContext().popCurrentPrivType();
        }

        return this;
    }

    /**
     * Returns the type of activation this class generates.
     *
     * @return either (NEED_ROW_ACTIVATION | NEED_PARAM_ACTIVATION) or (NEED_ROW_ACTIVATION) depending on params
     */
    int activationKind() {
        Vector parameterList = getCompilerContext().getParameterList();
        /*
        ** We need rows for all types of DML activations.  We need parameters
        ** only for those that have parameters.
        */
        if (parameterList != null && !parameterList.isEmpty()) {
            return StatementNode.NEED_PARAM_ACTIVATION;
        } else {
            return StatementNode.NEED_ROW_ACTIVATION;
        }
    }

    /**
     * Generate the code to create the ParameterValueSet, if necessary, when constructing the activation.  Also generate
     * the code to call a method that will throw an exception if we try to execute without all the parameters being
     * set.
     *
     * @param acb The ActivationClassBuilder for the class we're building
     */

    void generateParameterValueSet(ActivationClassBuilder acb) throws StandardException {
        Vector parameterList = getCompilerContext().getParameterList();
        int numberOfParameters = (parameterList == null) ? 0 : parameterList.size();

        ParameterNode.generateParameterValueSet
                (acb, numberOfParameters, parameterList);
    }

    /**
     * Return default privilege needed for this node. Other DML nodes can override this method to set their own default
     * privilege.
     *
     * @return true if the statement is atomic
     */
    int getPrivType() {
        return Authorizer.SELECT_PRIV;
    }

    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        if (resultSet != null)
            resultSet.buildTree(tree, depth + 1);
    }
}
