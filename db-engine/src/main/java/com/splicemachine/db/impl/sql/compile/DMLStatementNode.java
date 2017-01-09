/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.sql.compile.subquery.SubqueryFlattening;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * A DMLStatementNode represents any type of DML statement: a cursor declaration, an INSERT statement, and UPDATE
 * statement, or a DELETE statement.  All DML statements have result sets, but they do different things with them.  A
 * SELECT statement sends its result set to the client, an INSERT statement inserts its result set into a table, a
 * DELETE statement deletes from a table the rows corresponding to the rows in its result set, and an UPDATE statement
 * updates the rows in a base table corresponding to the rows in its result set.
 */

public abstract class DMLStatementNode extends StatementNode {
    private static final Logger LOG = Logger.getLogger(DMLStatementNode.class);

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

        /* Perform subquery flattening if applicable. */
        SubqueryFlattening.flatten(this);

        resultSet = resultSet.preprocess(getCompilerContext().getNumTables(), null, null);
        if (this instanceof CursorNode) {
            removeUnusedColumns(resultSet);
        }
        // Evaluate expressions with constant operands here to simplify the
        // query tree and to reduce the runtime cost. Do it before optimize()
        // since the simpler tree may have more accurate information for
        // the optimizer. (Example: The selectivity for 1=1 is estimated to
        // 0.1, whereas the actual selectivity is 1.0. In this step, 1=1 will
        // be rewritten to TRUE, which is known by the optimizer to have
        // selectivity 1.0.)
        accept(new ConstantExpressionVisitor());

        resultSet = resultSet.optimize(getDataDictionary(), null, 1.0d);
        resultSet = resultSet.modifyAccessPaths();

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
     * 	VALUES myfunc(SELECT max(c.commitFunc()) FROM T)
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
            SanityManager.ASSERT(fromList.size() == 0,
                    "fromList.size() is expected to be 0, not " + fromList.size() +
                            " on return from RS.bindExpressions()");
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
            SanityManager.ASSERT(fromList.size() == 0,
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
        if (parameterList != null && parameterList.size() > 0) {
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

        if (numberOfParameters <= 0)
            return;

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

    /**
     * Remove unused columns for all result set. A column is considered to be used if
     * 1) a column is referenced by a columns from resultColumnList of top most result set
     * 2) a column is referenced by a predicates
     * 3) a column is in group by list
     * 4) a column is in order by list
     * 5) a column is in a distinct select node
     * 6) a column is a generated column for aggregation
     * 7) a column is referenced by having clause
     * 8) a column is referenced by window definition
     *
     * @param resultSet
     * @throws StandardException
     */
    private void removeUnusedColumns(ResultSetNode resultSet) throws StandardException {

        ResultColumnList rcList = resultSet.getResultColumns();
        Map<ResultColumn, Integer> refCountMap = new HashMap<>();
        Map<ResultColumn, ResultColumn> referenceMap = new HashMap<>();
        Map<ResultColumn, ResultColumnList> resultColumnListMap = new HashMap<>();
        Map<String, BaseColumnNode> baseColumnNodes =  new HashMap<>();
        Set<ResultColumnList> includeColumnLists = new HashSet<>();
        Set<ResultColumnList> resultColumnLists = getResultColumnLists(resultSet, includeColumnLists);

        processIncludeColumnList(rcList, includeColumnLists, baseColumnNodes);
        processGroupByColumns(resultSet, refCountMap, baseColumnNodes);
        processOrderByColumns(resultSet, refCountMap, baseColumnNodes);
        processPredicates(resultSet, refCountMap, baseColumnNodes);
        processAggregates(resultSet, refCountMap, baseColumnNodes);
        processResultColumnLists(resultColumnLists, referenceMap, refCountMap, resultColumnListMap);

        Set<ResultColumn> toRemove = collectUnusedColumns(refCountMap, resultColumnListMap, baseColumnNodes,
                referenceMap, includeColumnLists);
        if (toRemove.size() > 0) {
            removeUnusedColumns(toRemove, resultColumnListMap);
            RemoveUnusedBaseColumns(resultSet, baseColumnNodes, refCountMap);
        }
    }

    private void RemoveUnusedBaseColumns(ResultSetNode resultSet,
                                         Map<String, BaseColumnNode> baseColumnNodes,
                                         Map<ResultColumn, Integer> refCountMap) throws StandardException {


        Set<FromBaseTable> fromBaseTables = collectNodes(resultSet, FromBaseTable.class, false);

        int total = 0;
        for (FromBaseTable fromBaseTable : fromBaseTables) {
            ResultColumnList resultColumnList = fromBaseTable.getResultColumns();
            int count = 0;
            for (int i = 0; i < resultColumnList.size(); ++i) {
                ResultColumn resultColumn = resultColumnList.elementAt(i);
                BaseColumnNode n = resultColumn.getBaseColumnNode();
                String colName =  n.getTableName() + "." + n.getColumnName();

                Integer refCount = refCountMap.get(resultColumn);
                if (!baseColumnNodes.containsKey(colName) && (refCount == null || refCount ==0)) {
                    resultColumn.setUnreferenced();
                    count++;
                    total++;
                }
            }
            LOG.debug("removed " + count + " out of " + resultColumnList.size() + " base columns from " + fromBaseTable.getTableName());
        }

        LOG.debug("removed " + total + " columns from " + fromBaseTables.size() + " base tables");
    }
    private void processIncludeColumnList(ResultColumnList rcList,
                                          Set<ResultColumnList> includeColumnLists,
                                          Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {
        Set<ColumnReference> columnReferences = collectNodes(rcList, ColumnReference.class, false);
        for (ColumnReference cr : columnReferences) {
            ResultColumn resultColumn = cr.getSource();
            if (resultColumn != null) {
                BaseColumnNode baseColumnNode = resultColumn.getBaseColumnNode();
                if (baseColumnNode != null) {
                    String columnName = baseColumnNode.getTableName() + "." + baseColumnNode.getColumnName();
                    baseColumnNodes.put(columnName, baseColumnNode);
                }
            }
        }
        for (ResultColumnList resultColumnList : includeColumnLists) {
            for (int i = 0; i < resultColumnList.size(); ++i) {
                ResultColumn resultColumn = resultColumnList.elementAt(i);
                BaseColumnNode baseColumnNode = resultColumn.getBaseColumnNode();
                if (baseColumnNode != null) {
                    String columnName = baseColumnNode.getTableName() + "." + baseColumnNode.getColumnName();
                    baseColumnNodes.put(columnName, baseColumnNode);
                }
            }

        }
        includeColumnLists.add(rcList);
    }

    /**
     * Collect unused columns from all result set except child result set of set operators
     * A column shouls be removed if
     * 1) its reference count is 0
     * 2) it's not from top most result set
     * 3) it's not from a distinct select
     * 4) it's not a generated column
     * 5) its base column is not referenced
     *
     * @param refCountMap
     * @param resultColumnListMap
     * @param baseColumnNodes
     * @param referenceMap
     * @return
     */
    private Set<ResultColumn> collectUnusedColumns(Map<ResultColumn, Integer> refCountMap,
                                                   Map<ResultColumn, ResultColumnList> resultColumnListMap,
                                                   Map<String, BaseColumnNode> baseColumnNodes,
                                                   Map<ResultColumn, ResultColumn> referenceMap,
                                                   Set<ResultColumnList> includeColumnLists) {
        Set<ResultColumn> toRemove = new HashSet<>();
        boolean done = false;
        while (!done) {
            done = true;
            Object[] columns = refCountMap.keySet().toArray();
            for (Object o:columns) {
                ResultColumn resultColumn = (ResultColumn)o;
                Integer refCount = refCountMap.get(resultColumn);
                if (refCount == 0 ) {
                    BaseColumnNode baseColumnNode = resultColumn.getBaseColumnNode();
                    String columnName = baseColumnNode !=null ?
                            baseColumnNode.getTableName()+"."+baseColumnNode.getColumnName() : null;
                    ResultColumnList resultColumnList = resultColumnListMap.get(resultColumn);
                    if (!includeColumnLists.contains(resultColumnList) &&
                            baseColumnNode != null && !baseColumnNodes.containsKey(columnName) &&
                            !resultColumn.isGenerated()) {
                        refCountMap.remove(resultColumn);
                        toRemove.add(resultColumn);
                        ResultColumn referenceColumn = referenceMap.get(resultColumn);
                        if (referenceColumn != null) {
                            refCountMap.put(referenceColumn, refCountMap.get(referenceColumn)-1);
                            done = false;
                        }
                    }
                }
            }
        }
        return toRemove;
    }

    /**
     * Remove all ununsed columns
     * @param toRemove
     * @param resultColumnListMap
     * @throws StandardException
     */
    private void removeUnusedColumns(Set<ResultColumn> toRemove,
                                     Map<ResultColumn, ResultColumnList> resultColumnListMap)
            throws StandardException {

        Map<SetOperatorNode, List<Integer>> removedSetOperatorColumns = new HashMap<>();

        for (ResultColumn resultColumn:toRemove) {
            ResultColumnList resultColumnList = resultColumnListMap.get(resultColumn);
            if (resultColumnList != null) {
                resultColumnList.removeElement(resultColumn);
            }
        }

        // Adjust virtual column Ids
        Set<ResultColumnList> allResultColumnList = collectNodes(resultSet, ResultColumnList.class, false);
        for (ResultColumnList resultColumnList : allResultColumnList) {
            for (int i = 0; i < resultColumnList.size(); ++i) {
                ResultColumn resultColumn = resultColumnList.elementAt(i);
                resultColumn.setVirtualColumnId(i+1);
            }
        }
    }

    /**
     *  Get all result columns that are referenced
     * @param node
     * @return
     * @throws StandardException
     */
    private List<ResultColumn> getTargetColumns(ValueNode node) throws StandardException{

        List<ResultColumn> targetColumns = Lists.newArrayList();
        if (node == null)
            return targetColumns;

        if (node instanceof VirtualColumnNode) {
            targetColumns.add(((VirtualColumnNode) node).getSourceColumn());
        }
        if (targetColumns.size() == 0) {
            targetColumns = CollectingVisitorBuilder
                    .forClass(ResultColumn.class)
                    .skipChildrenAfterCollect(true)
                    .collect(node);
        }

        if (targetColumns.size() == 0) {
            List<ColumnReference>  columnReferences = CollectingVisitorBuilder
                    .forClass(ColumnReference.class)
                    .skipChildrenAfterCollect(true)
                    .collect(node);
            for (ColumnReference cr:columnReferences) {
                ResultColumn rc = cr.getSource();
                if (cr != null) {
                    targetColumns.add(rc);
                }
            }
        }

        return targetColumns;
    }

    /**
     * Collect a set of specified nodes from parse tree
     * @param node root node
     * @param clazz class name
     * @param skipChildrenAfterCollect whether skip all child when a node is collected
     * @param <N>
     * @return
     * @throws StandardException
     */
    private <N> Set<N> collectNodes(Visitable node, Class<N> clazz, boolean skipChildrenAfterCollect) throws StandardException{
        List<N> l = CollectingVisitorBuilder
                .forClass(clazz)
                .skipChildrenAfterCollect(skipChildrenAfterCollect)
                .collect(node);
        Set<N> s = new HashSet<>();
        for (N e:l) {
            s.add(e);
        }
        return s;
    }

    /**
     * calculate reference count for each result column
     * @param resultColumnLists
     * @param referenceMap
     * @param refCountMap
     * @param resultColumnListMap
     * @throws StandardException
     */
    private void processResultColumnLists(Set<ResultColumnList> resultColumnLists,
                                          Map<ResultColumn, ResultColumn> referenceMap,
                                          Map<ResultColumn, Integer> refCountMap,
                                          Map<ResultColumn, ResultColumnList> resultColumnListMap) throws StandardException {

        for (ResultColumnList resultColumnList : resultColumnLists) {
            for (int i = 0; i < resultColumnList.size(); ++i) {
                ResultColumn resultColumn = resultColumnList.elementAt(i);
                resultColumnListMap.put(resultColumn, resultColumnList);

                List<ResultColumn> targetColumns = getTargetColumns(resultColumn.getExpression());
                for (ResultColumn rc : targetColumns) {
                    if (refCountMap.get(resultColumn) == null) {
                        refCountMap.put(resultColumn, 0);
                    }
                    referenceMap.put(resultColumn, rc);
                    Integer refCount = refCountMap.get(rc);
                    if (refCount == null) {
                        refCountMap.put(rc, 1);
                    }
                    else {
                        refCountMap.put(rc, refCount+1);
                    }
                }
            }
        }
    }

    /**
     *  Process where clause. Record referenced result column and base columns
     * @param resultSet
     * @param refCountMap
     * @param baseColumnNodes
     * @throws StandardException
     */
    private void processPredicates(ResultSetNode resultSet,
                                    Map<ResultColumn, Integer> refCountMap,
                                    Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {

        Set<Predicate> predicates = collectNodes(resultSet, Predicate.class, false);
        for (Predicate predicate : predicates) {
            Set<ColumnReference> columnReferences = collectNodes(predicate, ColumnReference.class, false);
            for (ColumnReference cr : columnReferences) {
                ResultColumn resultColumn = cr.getSource();
                addColumnReferences(resultColumn, refCountMap, baseColumnNodes);
            }
        }
    }

    /**
     * Process group by columns. Record referenced result column and base columns
     * @param resultSet
     * @param refCountMap
     * @param baseColumnNodes
     * @throws StandardException
     */
    private void processGroupByColumns(ResultSetNode resultSet,
                                       Map<ResultColumn, Integer> refCountMap,
                                       Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {

        Set<SelectNode> selectNodes = collectNodes(resultSet, SelectNode.class, false);
        for (SelectNode selectNode: selectNodes) {
            GroupByList groupByList = selectNode.getGroupByList();
            if (groupByList != null) {
                for (int i = 0; i < groupByList.size(); ++i) {
                    GroupByColumn groupByColumn = groupByList.getGroupByColumn(i);
                    Set<ColumnReference> columnReferences = collectNodes(groupByColumn, ColumnReference.class, false);
                    for (ColumnReference cr : columnReferences) {
                        ResultColumn resultColumn = cr.getSource();
                        addColumnReferences(resultColumn, refCountMap, baseColumnNodes);
                    }
                }
            }

            WindowList windowNodes = selectNode.windowDefinitionList;
            if (windowNodes != null) {
                for (int i = 0; i <windowNodes.size(); ++i) {
                    WindowDefinitionNode n = (WindowDefinitionNode) windowNodes.elementAt(i);
                    List<OrderedColumn> keyColumns = n.getOverColumns();
                    for (OrderedColumn col : keyColumns) {
                        ValueNode vn = col.getColumnExpression();
                        Set<ColumnReference> columnReferences = collectNodes(vn, ColumnReference.class, false);
                        for (ColumnReference cr : columnReferences) {
                            ResultColumn resultColumn = cr.getSource();
                            addColumnReferences(resultColumn, refCountMap, baseColumnNodes);
                        }
                    }
                }
            }
        }
    }

    /**
     * Process order by columns. Record referenced result column and base columns
     * @param resultSet
     * @param refCountMap
     * @param baseColumnNodes
     * @throws StandardException
     */
    private void processOrderByColumns(ResultSetNode resultSet,
                                       Map<ResultColumn, Integer> refCountMap,
                                       Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {
        Set<SelectNode> selectNodes = collectNodes(resultSet, SelectNode.class, false);
        for(SelectNode selectNode : selectNodes) {
            OrderByList orderByList = selectNode.getOrderByList();
            if (orderByList != null) {
                for (int i = 0; i < orderByList.size(); ++i) {
                    OrderByColumn orderByColumn = orderByList.getOrderByColumn(i);
                    ResultColumn resultColumn = orderByColumn.getResultColumn();
                    addColumnReferences(resultColumn, refCountMap, baseColumnNodes);
                }
            }
        }
    }

    /**
     * Process join clause. Record referenced result column and base columns
     * @param resultSet
     * @param refCountMap
     * @param baseColumnNodes
     * @throws StandardException
     */
    private void processAggregates(ResultSetNode resultSet,
                                   Map<ResultColumn, Integer> refCountMap,
                                   Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {
        Set<SelectNode> selectNodes = collectNodes(resultSet, SelectNode.class, false);
        for (SelectNode selectNode : selectNodes) {
            processAggregateList(selectNode.getHavingAggregates(), refCountMap, baseColumnNodes);
            processAggregateList(selectNode.getSelectAggregates(), refCountMap, baseColumnNodes);
            processAggregateList(selectNode.getWhereAggregates(), refCountMap, baseColumnNodes);
        }
    }

    /**
     * Process aggregation list. Record referenced result column and base columns
     * @param aggregates
     * @param refCountMap
     * @param baseColumnNodes
     * @throws StandardException
     */
    private void processAggregateList(List<AggregateNode> aggregates,
                                      Map<ResultColumn, Integer> refCountMap,
                                      Map<String, BaseColumnNode> baseColumnNodes) throws StandardException {
        if (aggregates == null)
            return;

        for (AggregateNode aggregateNode : aggregates) {
            if (aggregateNode.getOperand() != null) {
                Set<ColumnReference> columnReferences = collectNodes(aggregateNode, ColumnReference.class, false);
                for (ColumnReference cr: columnReferences) {
                    ResultColumn resultColumn = cr.getSource();
                    addColumnReferences(resultColumn, refCountMap, baseColumnNodes);
                }
            }
        }

    }

    private void addColumnReferences(ResultColumn resultColumn,
                                     Map<ResultColumn, Integer> refCountMap,
                                     Map<String, BaseColumnNode> baseColumnNodes) {
        if (resultColumn == null)
            return;


        BaseColumnNode baseColumnNode = resultColumn.getBaseColumnNode();
        if(baseColumnNode != null) {
            String columnName = baseColumnNode.getTableName() + "." + baseColumnNode.getColumnName();
            baseColumnNodes.put(columnName, baseColumnNode);
            Integer refCount = refCountMap.get(resultColumn);
            if (refCount == null) {
                refCountMap.put(resultColumn, 1);
            } else {
                refCountMap.put(resultColumn, refCount + 1);
            }
        }
    }

    /**
     * Collect result column lists that will be used to calculated reference count for result columns. Exceptions:
     *
     * 1) Result column list of FromBaseTable. They are needed during optimization, and unused columns will be removed
     *    later.
     * 2) Result column list of SetOperatorNode's child result set. Unused column will be inferenced from the parent.
     * 3) Result column list of distinct select cannot be removed
     *
     * @param resultSet
     * @param includeColumnLists
     * @return
     * @throws StandardException
     */
    private Set<ResultColumnList> getResultColumnLists(ResultSetNode resultSet,
                                                       Set<ResultColumnList> includeColumnLists) throws StandardException {
        Set<ResultColumnList> resultColumnLists = collectNodes(resultSet, ResultColumnList.class, false);
        Set<ResultSetNode> resultSetNodes = collectNodes(resultSet, ResultSetNode.class, false);

        //Do not remove columns from FromBaseTable now
        for (ResultSetNode resultSetNode : resultSetNodes) {
            if (resultSetNode instanceof FromBaseTable) {
                ResultColumnList resultColumnList = resultSetNode.getResultColumns();
                resultColumnLists.remove(resultColumnList);
            }
        }

        // Do not remove columns from distinct select because they are always needed for sorting
        Set<SelectNode> selectNodes = collectNodes(resultSet, SelectNode.class, false);
        for (SelectNode selectNode : selectNodes) {
            if (selectNode.hasDistinct()) {
                ResultColumnList resultColumnList = selectNode.getResultColumns();
                includeColumnLists.add(resultColumnList);
            }
        }

        // Collect set operator nodes
        Set<SetOperatorNode> setOperators = collectNodes(resultSet, SetOperatorNode.class, false);
        for (SetOperatorNode setOperator:setOperators) {
            includeColumnLists.add(setOperator.getResultColumns());
            ResultSetNode leftChild = setOperator.getLeftResultSet();
            includeColumnLists.add(leftChild.getResultColumns());

            ResultSetNode rightChild = setOperator.getRightResultSet();
            includeColumnLists.add(rightChild.getResultColumns());
        }
        return resultColumnLists;
    }
}
