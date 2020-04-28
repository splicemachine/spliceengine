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

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.util.StringUtil;
import org.apache.spark.sql.types.StructField;

import java.util.Collections;
import java.util.List;

/**
 * A ResultColumn represents a result column in a SELECT, INSERT, or UPDATE
 * statement.  In a SELECT statement, the result column just represents an
 * expression in a row being returned to the client.  For INSERT and UPDATE
 * statements, the result column represents an column in a stored table.
 * So, a ResultColumn has to be bound differently depending on the type of
 * statement it appears in.
 * <P>
 * The type of the ResultColumn can differ from its underlying expression,
 * for example in certain joins the ResultColumn can be nullable even if
 * its underlying column is not. In an INSERT or UPDATE the ResultColumn
 * will represent the type of the column in the table, the type of
 * the underlying expression will be the type of the source of the
 * value to be insert or updated. The method columnTypeAndLengthMatch()
 * can be used to detect when normalization is required between
 * the expression and the type of ResultColumn. This class does
 * not implement any type normalization (conversion), this is
 * typically handled by a NormalizeResultSetNode.
 *
 */

public class ResultColumn extends ValueNode
        implements ResultColumnDescriptor, Comparable
{
    /* name and exposedName should point to the same string, unless there is a
     * derived column list, in which case name will point to the underlying name
     * and exposedName will point to the name from the derived column list.
     */
    String            name;
    String            exposedName;
    String            tableName;
    String            sourceTableName;
    //Used by metadata api ResultSetMetaData.getSchemaName to get a column's table's schema.
    String            sourceSchemaName;
    ValueNode        expression;
    ColumnDescriptor    columnDescriptor;
    boolean            isGenerated;
    boolean            isGeneratedForUnmatchedColumnInInsert;
    boolean            isGroupingColumn;
    boolean            isReferenced;
    boolean            isRedundant;
    boolean            isNameGenerated;
    boolean            updated;
    boolean            updatableByCursor;
    private boolean defaultColumn;
    private boolean wasDefault;
    private boolean isPulledupOrderingColumn;
    private boolean fromLeftChild = true;

    //Following 2 fields have been added for DERBY-4631.
    //rightOuterJoinUsingClause will be set to true for following 2 cases
    //1)if this column represents the join column which is part of the
    //  SELECT list of a RIGHT OUTER JOIN with USING/NATURAL. eg
    //     select c from t1 right join t2 using (c)
    //  This case is talking about column c as in "select c"
    //2)if this column represents the join column from the right table
    //  for predicates generated for the USING/NATURAL of RIGHT OUTER JOIN
    //  eg
    //     select c from t1 right join t2 using (c)
    //  For "using(c)", a join predicate will be created as follows
    //    t1.c=t2.c
    //  This case is talking about column t2.c of the join predicate.
    private boolean rightOuterJoinUsingClause;
    //Following will be non-null for the case 1) above. It will show the
    // association of this result column to the join resultset created
    // for the RIGHT OUTER JOIN with USING/NATURAL. This information along
    // with rightOuterJoinUsingClause will be used during the code generation
    // time.
    private JoinNode joinResultSet = null;

    // tells us if this ResultColumn is a placeholder for a generated
    // autoincrement value for an insert statement.
    boolean            autoincrementGenerated;

    // tells us if this ResultColumn represents an autoincrement column in a
    // base table.
    boolean         autoincrement;

    /* ResultSetNumber for the ResultSet (at generate() time) that we belong to */
    private int        resultSetNumber = -1;
    ColumnReference reference; // used to verify quals at bind time, if given.

    /* virtualColumnId is the ResultColumn's position (1-based) within the ResultSet */
    private int        virtualColumnId;

    /**
     * Different types of initializer parameters indicate different
     * types of initialization. Parameters may be:
     *
     * <ul>
     * <li>arg1    The name of the column, if any.</li>
     * <li>arg2    The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1    a column reference node</li>
     * <li>arg2    The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1    The column descriptor.</li>
     * <li>arg2    The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>dtd            The type of the column</li>
     * <li>expression    The expression this result column represents</li>
     * </ul>
     */
    public void init(Object arg1, Object arg2) throws StandardException
    {
        // RESOLVE: This is something of a hack - it is not obvious that
        // the first argument being null means it should be treated as
        // a String.
        if ((arg1 instanceof String) || (arg1 == null))
        {
            this.name = (String) arg1;
            this.exposedName = this.name;
            setExpression( (ValueNode) arg2 );
        }
        else if (arg1 instanceof ColumnReference)
        {
            ColumnReference ref = (ColumnReference) arg1;

            this.name = ref.getColumnName();
            this.exposedName = ref.getColumnName();
            /*
                when we bind, we'll want to make sure
                the reference has the right table name.
             */
            this.reference = ref;
            setExpression( (ValueNode) arg2 );
        }
        else if (arg1 instanceof ColumnDescriptor)
        {
            ColumnDescriptor coldes = (ColumnDescriptor) arg1;

            this.name = coldes.getColumnName();
            this.exposedName = name;
            setType(coldes.getType());
            this.columnDescriptor = coldes;
            setExpression( (ValueNode) arg2 );
            this.autoincrement = coldes.isAutoincrement();
        }
        else
        {
            setType((DataTypeDescriptor) arg1);
            setExpression( (ValueNode) arg2 );
            if (arg2 instanceof ColumnReference)
            {
                reference = (ColumnReference) arg2;
            }
        }

        /* this result column represents a <default> keyword in an insert or
         * update statement
         */
        if (expression != null &&
                expression.isInstanceOf(C_NodeTypes.DEFAULT_NODE))
            defaultColumn = true;
    }

    /**
     * Returns TRUE if the ResultColumn is join column for a RIGHT OUTER
     *  JOIN with USING/NATURAL. More comments at the top of this class
     *  where rightOuterJoinUsingClause is defined.
     */
    public boolean isRightOuterJoinUsingClause()
    {
        return rightOuterJoinUsingClause;
    }

    /**
     * Will be set to TRUE if this ResultColumn is join column for a
     *  RIGHT OUTER JOIN with USING/NATURAL. More comments at the top of
     *  this class where rightOuterJoinUsingClause is defined. 2 eg cases
     * 1)select c from t1 right join t2 using (c)
     *   This case is talking about column c as in "select c"
     * 2)select c from t1 right join t2 using (c)
     *   For "using(c)", a join predicate will be created as follows
     *     t1.c=t2.c
     *   This case is talking about column t2.c of the join predicate.
     *
     * This method gets called for Case 1) during the bind phase of
     *  ResultColumn(ResultColumn.bindExpression).
     *
     * This method gets called for Case 2) during the bind phase of
     *  JoinNode while we are going through the list of join columns
     *  for a NATURAL JOIN or user supplied list of join columns for
     *  USING clause(JoinNode.getMatchingColumn).
     *
     * @param value True/False
     */
    public void setRightOuterJoinUsingClause(boolean value)
    {
        rightOuterJoinUsingClause = value;
    }

    /**
     * Returns a non-null value if the ResultColumn represents the join
     * column which is part of the SELECT list of a RIGHT OUTER JOIN with
     * USING/NATURAL. eg
     *      select c from t1 right join t2 using (c)
     * The join column we are talking about is column c as in "select c"
     * The return value of following method will show the association of this
     * result column to the join resultset created for the RIGHT OUTER JOIN
     * with USING/NATURAL. This information along with
     * rightOuterJoinUsingClause will be used during the code generation
     * time.
     */
    public JoinNode getJoinResultSet() {
        return joinResultSet;
    }

    /**
     * This method gets called during the bind phase of a ResultColumn if it
     *  is determined that the ResultColumn represents the join column which
     *  is part of the SELECT list of a RIGHT OUTER JOIN with
     *  USING/NATURAL. eg
     *      select c from t1 right join t2 using (c)
     *   This case is talking about column c as in "select c"
     * @param resultSet - The ResultColumn belongs to this JoinNode
     */
    public void setJoinResultset(JoinNode resultSet)
    {
        joinResultSet = resultSet;
    }

    /**
     * Returns TRUE if the ResultColumn is standing in for a DEFAULT keyword in
     * an insert/update statement.
     */
    public boolean isDefaultColumn()
    {
        return defaultColumn;
    }

    public void setDefaultColumn(boolean value)
    {
        defaultColumn = value;
    }

    /**
     * Returns TRUE if the ResultColumn used to stand in for a DEFAULT keyword in
     * an insert/update statement.
     */
    public boolean wasDefaultColumn()
    {
        return wasDefault;
    }

    public void setWasDefaultColumn(boolean value)
    {
        wasDefault = value;
    }

    /**
     * Return TRUE if this result column matches the provided column name.
     *
     * This function is used by ORDER BY column resolution. For the
     * ORDER BY clause, Derby will prefer to match on the column's
     * alias (exposedName), but will also successfully match on the
     * underlying column name. Thus the following statements are
     * treated equally:
     *  select name from person order by name;
     *  select name as person_name from person order by name;
     *  select name as person_name from person order by person_name;
     * See DERBY-2351 for more discussion.
     */
    boolean columnNameMatches(String columnName)
    {
        return columnName.equals(exposedName) ||
                columnName.equals(name) ||
                columnName.equals(getSourceColumnName());
    }

    /**
     * Get non-null column name. This method is called during the bind phase
     *  to see if we are dealing with ResultColumn in the SELECT list that
     *  belongs to a RIGHT OUTER JOIN(NATURAL OR USING)'s join column.
     *
     * For a query like following, we want to use column name x and not the
     *  alias x1 when looking in the JoinNode for join column
     *   SELECT x x1
     *        FROM derby4631_t2 NATURAL RIGHT OUTER JOIN derby4631_t1;
     * For a query like following, getSourceColumnName() will return null
     *  because we are dealing with a function for the column. For this
     *  case, "name" will return the alias name cx
     *   SELECT coalesce(derby4631_t2.x, derby4631_t1.x) cx
     *        FROM derby4631_t2 NATURAL RIGHT OUTER JOIN derby4631_t1;
     * For a query like following, getSourceColumnName() and name will
     *  return null and hence need to use the generated name
     *   SELECT ''dummy="'|| TRIM(CHAR(x))|| '"'
     *        FROM (derby4631_t2 NATURAL RIGHT OUTER JOIN derby4631_t1);
     */
    String getUnderlyingOrAliasName()
    {
        if (getSourceColumnName() != null)
            return getSourceColumnName();
        else if (name != null)
            return name;
        else
            return exposedName;
    }

    /**
     * Returns the underlying source column name, if this ResultColumn
     * is a simple direct reference to a table column, or NULL otherwise.
     */
    String getSourceColumnName()
    {
        if (expression instanceof ColumnReference)
            return ((ColumnReference)expression).getColumnName();
        return null;
    }
    /**
     * The following methods implement the ResultColumnDescriptor
     * interface.  See the Language Module Interface for details.
     */

    public String getName()
    {
        return exposedName;
    }

    public String getFullName(){
        return getTableName() + "." + exposedName;
    }

    public String getSchemaName() throws StandardException
    {
        if ((columnDescriptor!=null) &&
                (columnDescriptor.getTableDescriptor() != null))
            return columnDescriptor.getTableDescriptor().getSchemaName();
        else
        {
            if (expression != null)
                // REMIND: could look in reference, if set.
                return expression.getSchemaName();
            else
                return null;
        }
    }

    public String getTableName()
    {
        if (tableName != null)
        {
            return tableName;
        }
        if ((columnDescriptor!=null) &&
                (columnDescriptor.getTableDescriptor() != null))
        {
            return columnDescriptor.getTableDescriptor().getName();
        }
        else
        {
            return expression.getTableName();
        }
    }

    /**
     * @see ResultColumnDescriptor#getSourceTableName
     */
    public String getSourceTableName()
    {
        return sourceTableName;
    }

    /**
     * @see ResultColumnDescriptor#getSourceSchemaName
     */
    public String getSourceSchemaName()
    {
        return sourceSchemaName;
    }

    /**
     * Clear the table name for the underlying ColumnReference.
     * See UpdateNode.scrubResultColumns() for full explaination.
     */
    public void clearTableName()
    {
        if (expression instanceof ColumnReference)
        {
            ((ColumnReference) expression).setTableNameNode((TableName) null);
        }
    }

    public DataTypeDescriptor getType()
    {
        return getTypeServices();
    }

    public int getColumnPosition() {
        if (columnDescriptor!=null)
            return columnDescriptor.getPosition();
        else
            return virtualColumnId;
    }

    public int getStoragePosition() {
        if (columnDescriptor!=null)
            return columnDescriptor.getStoragePosition();
        else
            return virtualColumnId;
    }


    /**
     * Set the expression in this ResultColumn.  This is useful in those
     * cases where you don't know the expression in advance, like for
     * INSERT statements with column lists, where the column list and
     * SELECT or VALUES clause are parsed separately, and then have to
     * be hooked up.
     *
     * @param expression    The expression to be set in this ResultColumn
     */

    public void setExpression(ValueNode expression)
    {
        this.expression = expression;
    }

    /**
     * Get the expression in this ResultColumn.
     *
     * @return ValueNode    this.expression
     */

    public ValueNode getExpression()
    {
        return expression;
    }

    /**
     * Set the expression to a null node of the
     * correct type.
     *
     * @exception StandardException        Thrown on error
     */
    void setExpressionToNullNode()
            throws StandardException
    {
        setExpression( getNullNode(getTypeServices()) );
    }

    /**
     * Set the name in this ResultColumn.  This is useful when you don't
     * know the name at the time you create the ResultColumn, for example,
     * in an insert-select statement, where you want the names of the
     * result columns to match the table being inserted into, not the
     * table they came from.
     *
     * @param name    The name to set in this ResultColumn
     */

    public void setName(String name)
    {
        if (this.name == null)
        {
            this.name = name;
        }
        else {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(reference == null ||
                                name.equals(reference.getColumnName()),
                        "don't change name from reference name");
        }

        this.exposedName = name;
    }

    /**
     * Is the name for this ResultColumn generated?
     */
    public boolean isNameGenerated()
    {
        return isNameGenerated;
    }

    /**
     * Set that this result column name is generated.
     */
    public void setNameGenerated(boolean value)
    {
        isNameGenerated = value;
    }

    /**
     * Set the resultSetNumber for this ResultColumn.  This is the
     * resultSetNumber for the ResultSet that we belong to.  This
     * is useful for generate() and necessary since we do not have a
     * back pointer to the RSN.
     *
     * @param resultSetNumber    The resultSetNumber.
     */
    public void setResultSetNumber(int resultSetNumber)
    {
        this.resultSetNumber = resultSetNumber;
    }

    /**
     * Get the resultSetNumber for this ResultColumn.
     *
     * @return int    The resultSetNumber.
     */
    public int getResultSetNumber()
    {
        return resultSetNumber;
    }

    /**
     * Adjust the virtualColumnId for this ResultColumn    by the specified amount
     *
     * @param adjust    The adjustment for the virtualColumnId
     */

    public void adjustVirtualColumnId(int adjust)
    {
        virtualColumnId += adjust;
    }

    /**
     * Set the virtualColumnId for this ResultColumn
     *
     * @param id    The virtualColumnId for this ResultColumn
     */

    public void setVirtualColumnId(int id)
    {
        virtualColumnId = id;
    }

    /**
     * Get the virtualColumnId for this ResultColumn
     *
     * @return virtualColumnId for this ResultColumn
     */
    public int getVirtualColumnId()
    {
        return virtualColumnId;
    }

    /**
     * Adjust this virtualColumnId to account for the removal of a column
     *
     * This routine is called when bind processing finds and removes
     * duplicate columns in the result list which were pulled up due to their
     * presence in the ORDER BY clause, but were later found to be duplicate.
     *
     * If this column is a virtual column, and if this column's virtual
     * column id is greater than the column id which is being removed, then
     * we must logically shift this column to the left by decrementing its
     * virtual column id.
     *
     * @param removedColumnId   id of the column being removed.
     */
    public void collapseVirtualColumnIdGap(int removedColumnId)
    {
        if (columnDescriptor == null && virtualColumnId > removedColumnId)
            virtualColumnId--;
    }

    /**
     * Generate a unique (across the entire statement) column name for unnamed
     * ResultColumns
     *
     * @exception StandardException        Thrown on error
     */
    public void guaranteeColumnName() throws StandardException
    {
        if (exposedName == null)
        {
            /* Unions may also need generated names, if both sides name don't match */
            exposedName ="SQLCol" + getCompilerContext().getNextColumnNumber();
            isNameGenerated = true;
        }
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "exposedName: " + exposedName + "\n" +
                    "name: " + name + "\n" +
                    "tableName: " + tableName + "\n" +
                    "isDefaultColumn: " + defaultColumn + "\n" +
                    "wasDefaultColumn: " + wasDefault + "\n" +
                    "isNameGenerated: " + isNameGenerated + "\n" +
                    "sourceTableName: " + sourceTableName + "\n" +
                    "type: " + getTypeServices() + "\n" +
                    "columnDescriptor: " + columnDescriptor + "\n" +
                    "isGenerated: " + isGenerated + "\n" +
                    "isGeneratedForUnmatchedColumnInInsert: " + isGeneratedForUnmatchedColumnInInsert + "\n" +
                    "isGroupingColumn: " + isGroupingColumn + "\n" +
                    "isReferenced: " + isReferenced + "\n" +
                    "isRedundant: " + isRedundant + "\n" +
                    "rightOuterJoinUsingClause: " + rightOuterJoinUsingClause + "\n" +
                    "joinResultSet: " + joinResultSet + "\n" +
                    "virtualColumnId: " + virtualColumnId + "\n" +
                    "resultSetNumber: " + resultSetNumber + "\n" +
                    super.toString();
        }
        else
        {
            return "";
        }
    }

    @Override
    public String toHTMLString() {
        return "" +
                "exposedName: " + exposedName + "<br>" +
                "name: " + name + "<br>" +
                "tableName: " + tableName + "<br>" +
                "resultSetNumber: " + resultSetNumber + "<br>" +
                "virtualColumnId: " + virtualColumnId + "<br>" +
                "isDefaultColumn: " + defaultColumn + "<br>" +
                "wasDefaultColumn: " + wasDefault + "<br>" +
                "isNameGenerated: " + isNameGenerated + "<br>" +
                "sourceTableName: " + sourceTableName + "<br>" +
                "columnDescriptor: " + columnDescriptor + "<br>" +
                "isGenerated: " + isGenerated + "<br>" +
                "isGeneratedForUnmatchedColumnInInsert: " + isGeneratedForUnmatchedColumnInInsert + "<br>" +
                "isGroupingColumn: " + isGroupingColumn + "<br>" +
                "isReferenced: " + isReferenced + "<br>" +
                "isRedundant: " + isRedundant + "<br>" +
                "rightOuterJoinUsingClause: " + rightOuterJoinUsingClause + "<br>" +
                "joinResultSet: " + joinResultSet + "<br>" +
                "identityHashCode: " + System.identityHashCode(this) + "<br>" +
                super.toHTMLString();
    }


    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);
            if (expression != null)
            {
                printLabel(depth, "expression: ");
                expression.treePrint(depth + 1);
            }
            if (reference != null)
            {
                printLabel(depth, "reference: ");
                reference.treePrint(depth + 1);
            }
        }
    }

    /**
     * Bind this expression.  This means binding the sub-expressions.
     * In this case, we figure out what the result type of this result
     * column is when we call one of the bindResultColumn*() methods.
     * The reason is that there are different ways of binding the
     * result columns depending on the statement type, and this is
     * a standard interface that does not take the statement type as
     * a parameter.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>    aggregateVector) throws StandardException {
        /*
        ** Set the type of a parameter to the type of the result column.
        ** Don't do it if this result column doesn't have a type yet.
        ** This can happen if the parameter is part of a table constructor.
        */
        if (expression.requiresTypeFromContext()) {
            if (getTypeServices() != null) {
                expression.setType(getTypeServices());
            }
        }

        //DERBY-4631
        //Following code is for a join column(which obviously will not be
        // qualified with a table name because join columns are not
        // associated with left or right table) of RIGHT OUTER JOIN
        // with USING/NATURAL join. For such columns,
        // isJoinColumnForRightOuterJoin() call will set
        // rightOuterJoinUsingClause to true and associate the
        // JoinResultSet with it. eg
        //      select c from t1 right join t2 using (c)
        // Here, we are talking about column c as in "select c"
        if (expression.getTableName() == null) {
            fromList.isJoinColumnForRightOuterJoin(this);
        }

        ValueNode newExpression =
            expression.bindExpression(fromList, subqueryList, aggregateVector);

        if (newExpression instanceof BinaryLogicalOperatorNode ||
            newExpression instanceof UnaryLogicalOperatorNode)
            newExpression = SelectNode.normExpressions(newExpression);
        setExpression( newExpression );

        if (expression instanceof ColumnReference)
        {
            autoincrement = ((ColumnReference)expression).getSource().isAutoincrement();
        }

        return this;
    }

    /**
     * Bind this result column by ordinal position and set the VirtualColumnId.
     * This is useful for INSERT statements like "insert into t values (1, 2, 3)",
     * where the user did not specify a column list.
     * If a columnDescriptor is not found for a given position, then
     * the user has specified more values than the # of columns in
     * the table and an exception is thrown.
     *
     * NOTE: We must set the VirtualColumnId here because INSERT does not
     * construct the ResultColumnList in the usual way.
     *
     * @param tableDescriptor    The descriptor for the table being
     *                inserted into
     * @param columnId        The ordinal position of the column
     *                        in the table, starting at 1.
     *
     * @exception StandardException        Thrown on error
     */

    void bindResultColumnByPosition(TableDescriptor tableDescriptor,
                                    int columnId)
            throws StandardException
    {
        ColumnDescriptor    columnDescriptor;

        columnDescriptor = tableDescriptor.getColumnDescriptor(columnId);

        if (columnDescriptor == null)
        {
            String        errorString;
            String        schemaName;

            errorString = "";
            schemaName = tableDescriptor.getSchemaName();
            if (schemaName != null)
                errorString += schemaName + ".";
            errorString += tableDescriptor.getName();

            throw StandardException.newException(SQLState.LANG_TOO_MANY_RESULT_COLUMNS, errorString);
        }

        setColumnDescriptor(tableDescriptor, columnDescriptor);
        setVirtualColumnId(columnId);
    }

    /**
     * Bind this result column by its name and set the VirtualColumnId.
     * This is useful for update statements, and for INSERT statements
     * like "insert into t (a, b, c) values (1, 2, 3)" where the user
     * specified a column list.
     * An exception is thrown when a columnDescriptor cannot be found for a
     * given name.  (There is no column with that name.)
     *
     * NOTE: We must set the VirtualColumnId here because INSERT does not
     * construct the ResultColumnList in the usual way.
     *
     * @param tableDescriptor    The descriptor for the table being
     *                updated or inserted into
     * @param columnId        The ordinal position of the column
     *                        in the table, starting at 1. (Used to
     *                        set the VirtualColumnId.)
     *
     * @exception StandardException        Thrown on error
     */

    public void bindResultColumnByName(TableDescriptor tableDescriptor,
                                       int columnId)
            throws StandardException
    {
        ColumnDescriptor    columnDescriptor;

        columnDescriptor = tableDescriptor.getColumnDescriptor(exposedName);

        if (columnDescriptor == null)
        {
            String        errorString;
            String        schemaName;

            errorString = "";
            schemaName = tableDescriptor.getSchemaName();
            if (schemaName != null)
                errorString += schemaName + ".";
            errorString += tableDescriptor.getName();

            throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, exposedName, errorString);
        }

        setColumnDescriptor(tableDescriptor, columnDescriptor);
        setVirtualColumnId(columnId);
        if (isPrivilegeCollectionRequired())
            getCompilerContext().addRequiredColumnPriv( columnDescriptor);
    }

    /**
     * Change an untyped null to a typed null.
     *
     * @exception StandardException        Thrown on error
     */
    public void typeUntypedNullExpression( ResultColumn bindingRC)
            throws StandardException
    {
        TypeId typeId = bindingRC.getTypeId();
        /* This is where we catch null in a VALUES clause outside
         * of INSERT VALUES()
         */
        if (typeId == null)
        {
            throw StandardException.newException(SQLState.LANG_NULL_IN_VALUES_CLAUSE);
        }

        if( expression instanceof UntypedNullConstantNode)
            //since we don't know the type of such a constant node, we just
            //use the default values for collation type and derivation.
            //eg insert into table1 values(1,null)
            //When this method is executed for the sql above, we don't know
            //the type of the null at this point.
            setExpression( getNullNode(bindingRC.getTypeServices()) );
        else if( ( expression instanceof ColumnReference) && expression.getTypeServices() == null)
        {
            // The expression must be a reference to a null column in a values table.
            expression.setType( bindingRC.getType());
        }
    }

    /**
     * Set the column descriptor for this result column.  It also gets
     * the data type services from the column descriptor and stores it in
     * this result column: this is redundant, but we have to store the result
     * type here for SELECT statements, and it is more orthogonal if the type
     * can be found here regardless of what type of statement it is.
     *
     * @param tableDescriptor    The TableDescriptor for the table
     *                being updated or inserted into.
     *                This parameter is used only for
     *                error reporting.
     * @param columnDescriptor    The ColumnDescriptor to set in
     *                this ResultColumn.
     *
     * @exception StandardException tableNameMismatch
     */
    void setColumnDescriptor(TableDescriptor tableDescriptor,
                             ColumnDescriptor columnDescriptor) throws StandardException
    {
        /* Callers are responsible for verifying that the column exists */
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(columnDescriptor != null,
                    "Caller is responsible for verifying that column exists");

        setType(columnDescriptor.getType());
        this.columnDescriptor = columnDescriptor;

        /*
            If the node was created using a reference, the table name
            of the reference must agree with that of the tabledescriptor.
         */
        if (reference != null && reference.getTableName() != null)
        {
            if (! tableDescriptor.getName().equals(
                    reference.getTableName()) )
            {
                /* REMIND: need to have schema name comparison someday as well...
                ** left out for now, lots of null checking needed...
                ** || ! tableDescriptor.getSchemaName().equals(
                **    reference.getTableNameNode().getSchemaName())) {
                */
                String realName = tableDescriptor.getName();
                String refName = reference.getTableName();

                throw StandardException.newException(SQLState.LANG_TABLE_NAME_MISMATCH,
                        realName, refName);
            }
        }
    }

    /**
     * Bind the result column to the expression that lives under it.
     * All this does is copy the datatype information to this node.
     * This is useful for SELECT statements, where the result type
     * of each column is the type of the column's expression.
     *
     * @exception StandardException        Thrown on error
     */
    public void bindResultColumnToExpression()
            throws StandardException
    {
        /*
        ** This gets the same DataTypeServices object as
        ** is used in the expression.  It is probably not
        ** necessary to clone the object here.
        */
        setType(expression.getTypeServices());

        if (expression instanceof ColumnReference)
        {
            ColumnReference cr = (ColumnReference) expression;
            tableName = cr.getTableName();
            sourceTableName = cr.getSourceTableName();
            sourceSchemaName = cr.getSourceSchemaName();
        }
    }


    /**
     * Set the column source's table name
     * @param t The source table name
     */
    public void setSourceTableName(String t) {
        sourceTableName = t;
    }

    /**
     * Set the column source's schema name
     * @param s The source schema name
     */
    public void setSourceSchemaName(String s) {
        sourceSchemaName = s;
    }

    /**
     * Preprocess an expression tree.  We do a number of transformations
     * here (including subqueries, IN lists, LIKE and BETWEEN) plus
     * subquery flattening.
     * NOTE: This is done before the outer ResultSetNode is preprocessed.
     *
     * @param    numTables            Number of tables in the DML Statement
     * @param    outerFromList        FromList from outer query block
     * @param    outerSubqueryList    SubqueryList from outer query block
     * @param    outerPredicateList    PredicateList from outer query block
     *
     * @return        The modified expression
     *
     * @exception StandardException        Thrown on error
     */
    public ValueNode preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList)
            throws StandardException
    {
        if (expression == null)
            return this;
        setExpression( expression.preprocess(numTables, outerFromList,
                outerSubqueryList,
                outerPredicateList) );
        return this;
    }

    /**
     This verifies that the expression is storable into the result column.
     It checks versus the given ResultColumn.

     This method should not be called until the result column and
     expression both have a valid type, i.e. after they are bound
     appropriately. Its use is for statements like insert, that need to
     verify if a given value can be stored into a column.

     @exception StandardException thrown if types not suitable.
     */
    public void checkStorableExpression(ResultColumn toStore)
            throws StandardException
    {
        checkStorableExpression((ValueNode) toStore);
    }

    private void checkStorableExpression(ValueNode source)
            throws StandardException
    {
        TypeId toStoreTypeId = source.getTypeId();

        if (!getTypeCompiler().storable(toStoreTypeId, getClassFactory()))
        {
            throw StandardException.newException(SQLState.LANG_NOT_STORABLE,
                    getTypeId().getSQLTypeName(),
                    toStoreTypeId.getSQLTypeName() );
        }
    }

    /**
     This verifies that the expression is storable into the result column.
     It checks versus the expression under this ResultColumn.

     This method should not be called until the result column and
     expression both have a valid type, i.e. after they are bound
     appropriately. Its use is for statements like update, that need to
     verify if a given value can be stored into a column.

     @exception StandardException thrown if types not suitable.
     */
    public void checkStorableExpression()
            throws StandardException
    {
        checkStorableExpression(getExpression());
    }

    /**
     * Do code generation for a result column.  This consists of doing the code
     * generation for the underlying expression.
     *
     * @param ecb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the expression will go into
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder ecb,
                                   MethodBuilder mb)
            throws StandardException
    {
        expression.generateExpression(ecb, mb);
    }

    /**
     * Do code generation to return a Null of the appropriate type
     * for the result column.
     Requires the getCOlumnExpress value pushed onto the stack
     *
     * @param acb        The ActivationClassBuilder for the class we're generating
     * @param eb        The ExpressionBlock that the generate code is to go into
     * @param getColumnExpression "fieldx.getColumn(y)"
     *
     * @exception StandardException        Thrown on error
     */
/*PUSHCOMPILE
    public void generateNulls(ExpressionClassBuilder acb,
                                    MethodBuilder mb,
                                    Expression getColumnExpress)
            throws StandardException
    {

        acb.pushDataValueFactory(mb);
        getTypeCompiler().generateNull(mb, acb.getBaseClassName());


        mb.cast(ClassName.DataValueDescriptor);


        return eb.newCastExpression(
                    ClassName.DataValueDescriptor,
                    getTypeCompiler().
                        generateNull(
                                    eb,
                                    acb.getBaseClassName(),
                                    acb.getDataValueFactory(eb),
                                    getColumnExpress));
    }
*/
    /**
     Generate the code to create a column the same shape and
     size as this ResultColumn.

     Used in ResultColumnList.generateHolder().

     @exception StandardException  thrown on failure
     */
    public void generateHolder(ExpressionClassBuilder acb,
                               MethodBuilder mb)
            throws StandardException
    {
        // generate expression of the form
        // (DataValueDescriptor) columnSpace

        acb.generateNull(mb, getTypeCompiler(), getTypeServices());
        mb.upCast(ClassName.DataValueDescriptor);
    }

    /**
     ** Check whether the column length and type of this result column
     ** match the expression under the columns.  This is useful for
     ** INSERT and UPDATE statements.  For SELECT statements this method
     ** should always return true.  There is no need to call this for a
     ** DELETE statement.
     **
     ** @return    true means the column matches its expressions,
     **            false means it doesn't match.
     */
    boolean columnTypeAndLengthMatch()
            throws StandardException
    {

        /*
        ** We can never make any assumptions about
        ** parameters.  So don't even bother in this
        ** case.
        */
        if (getExpression().requiresTypeFromContext())
        {
            return false;
        }

        // Are we inserting/updating an XML column?  If so, we always
        // return false so that normalization will occur.  We have to
        // do this because there are different "kinds" of XML values
        // and we need to make sure they match--but we don't know
        // the "kind" until execution time.  See the "normalize"
        // method in com.splicemachine.db.iapi.types.XML for more.
        if (getTypeId().isXMLTypeId())
            return false;


        DataTypeDescriptor  expressionType = getExpression().getTypeServices();

        if (!getTypeServices().isExactTypeAndLengthMatch(expressionType))
            return false;

        /* Is the source nullable and the target non-nullable? */
        return !((!getTypeServices().isNullable()) && expressionType.isNullable());

    }

    boolean columnTypeAndLengthMatch(ResultColumn otherColumn)
            throws StandardException
    {
        ValueNode otherExpression = otherColumn.getExpression();

        DataTypeDescriptor resultColumnType = getTypeServices();
        DataTypeDescriptor otherResultColumnType = otherColumn.getTypeServices();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(resultColumnType != null,
                    "Type is null for column " + this);
            SanityManager.ASSERT(otherResultColumnType != null,
                    "Type is null for column " + otherColumn);
        }

        /*
        ** We can never make any assumptions about
        ** parameters.  So don't even bother in this
        ** case.
        */
        if ((otherExpression != null) && (otherExpression.requiresTypeFromContext()) ||
                (expression.requiresTypeFromContext()))
        {
            return false;
        }

        // Are we inserting/updating an XML column?  If so, we always
        // return false so that normalization will occur.  We have to
        // do this because there are different "kinds" of XML values
        // and we need to make sure they match--but we don't know
        // the "kind" until execution time.  See the "normalize"
        // method in com.splicemachine.db.iapi.types.XML for more.
        if (resultColumnType.getTypeId().isXMLTypeId())
            return false;

        /* Are they the same type? */
        if ( ! resultColumnType.getTypeId().equals(
                otherResultColumnType.getTypeId()
        )
                )
        {
            /* If the source is a constant of a different type then
             * we try to convert that constant to a constant of our
             * type. (The initial implementation only does the conversion
             * to string types because the most common problem is a char
             * constant with a varchar column.)
             * NOTE: We do not attempt any conversion here if the source
             * is a string type and the target is not or vice versa in
             * order to avoid problems with implicit varchar conversions.
             * Anyway, we will check if the "converted" constant has the
             * same type as the original constant.  If not, then the conversion
             * happened.  In that case, we will reuse the ConstantNode, for simplicity,
             * and reset the type to match the desired type.
             */
            if (otherExpression instanceof ConstantNode)
            {
                ConstantNode constant = (ConstantNode)otherColumn.getExpression();
                DataValueDescriptor oldValue = constant.getValue();

                DataValueDescriptor newValue = convertConstant(
                        resultColumnType.getTypeId(),
                        resultColumnType.getMaximumWidth(),
                        oldValue);

                if ((oldValue != newValue) &&
                        (oldValue instanceof StringDataValue ==
                                newValue instanceof StringDataValue))
                {
                    constant.setValue(newValue);
                    constant.setType(getTypeServices());
                    otherColumn.bindResultColumnToExpression();
                    otherResultColumnType = otherColumn.getType();
                }
                //If we are dealing with StringDataValue, then make sure we
                //have correct collation type and derivaiton set and the value
                //represented by collation is either SQLxxx or CollatorSQLxxx
                //depending on the collation type.
                if (newValue instanceof StringDataValue)
                {
                    constant.setCollationInfo(resultColumnType);

                    DataValueFactory dvf = getDataValueFactory();
                    newValue = ((StringDataValue)newValue).getValue(dvf.getCharacterCollator(
                            constant.getTypeServices().getCollationType()));
                    constant.setValue(newValue);
                }
            }
            if ( ! resultColumnType.getTypeId().equals(
                    otherResultColumnType.getTypeId()
            )
                    )
            {
                return false;
            }
        }

        /* Are they the same precision? */
        if (resultColumnType.getPrecision() !=
                otherResultColumnType.getPrecision())
        {
            return false;
        }

        /* Are they the same scale? */
        if (resultColumnType.getScale() != otherResultColumnType.getScale())
        {
            return false;
        }

        /* Are they the same width? */
        if (resultColumnType.getMaximumWidth() !=
                otherResultColumnType.getMaximumWidth())
        {
            return false;
        }

        /* Is the source nullable and the target non-nullable?
         * The source is nullable if it is nullable or if the target is generated
         * for an unmatched column in an insert with a column list.
         * This additional check is needed because when we generate any additional
         * source RCs for an insert with a column list the generated RCs for any
         * non-specified columns get the type info from the column.  Thus,
         * for t1(non_nullable, nullable)
         *    insert into t2 (nullable) values 1;
         * RCType.isNullable() returns false for the generated source RC for
         * non_nullable.  In this case, we want to see it as
         */
        return !((!resultColumnType.isNullable()) &&
                (otherResultColumnType.isNullable() ||
                        otherColumn.isGeneratedForUnmatchedColumnInInsert()));

    }

    /**
     * Is this a generated column?
     *
     * @return Boolean - whether or not this column is a generated column.
     */
    public boolean isGenerated()
    {
        return (isGenerated);
    }

    /**
     * Is this columm generated for an unmatched column in an insert?
     *
     * @return Boolean - whether or not this columm was generated for an unmatched column in an insert.
     */
    public boolean isGeneratedForUnmatchedColumnInInsert()
    {
        return (isGeneratedForUnmatchedColumnInInsert);
    }

    /**
     * Mark this a columm as a generated column
     */
    public void markGenerated()
    {
        isGenerated = true;
        /* A generated column is a referenced column */
        isReferenced = true;
    }

    /**
     * Mark this a columm as generated for an unmatched column in an insert
     */
    public void markGeneratedForUnmatchedColumnInInsert()
    {
        isGeneratedForUnmatchedColumnInInsert = true;
        /* A generated column is a referenced column */
        isReferenced = true;
    }

    /**
     * Is this a referenced column?
     *
     * @return Boolean - whether or not this column is a referenced column.
     */
    public boolean isReferenced()
    {
        return isReferenced;
    }

    /**
     * Mark this column as a referenced column.
     */
    public void setReferenced()
    {
        isReferenced = true;
    }

    /**
     * Mark this column as a referenced column if it is already marked as referenced or if any result column in
     * its chain of virtual columns is marked as referenced.
     */
    void pullVirtualIsReferenced()
    {
        if( isReferenced())
            return;

        for( ValueNode expr = expression; expr != null && (expr instanceof VirtualColumnNode);)
        {
            VirtualColumnNode vcn = (VirtualColumnNode) expr;
            ResultColumn src = vcn.getSourceColumn();
            if( src.isReferenced())
            {
                setReferenced();
                return;
            }
            expr = src.getExpression();
        }
    } // end of pullVirtualIsReferenced

    /**
     * Mark this column as an unreferenced column.
     */
    public void setUnreferenced()
    {
        isReferenced = false;
    }

    /**
     * Mark this RC and all RCs in the underlying
     * RC/VCN chain as referenced.
     */
    void markAllRCsInChainReferenced()
    {
        setReferenced();

        ValueNode vn = expression;

        while (vn instanceof VirtualColumnNode)
        {
            VirtualColumnNode vcn = (VirtualColumnNode) vn;
            ResultColumn rc = vcn.getSourceColumn();
            rc.setReferenced();
            vn = rc.getExpression();
        }
    }

    /**
     * Is this a redundant ResultColumn?
     *
     * @return Boolean - whether or not this RC is redundant.
     */
    public boolean isRedundant()
    {
        return isRedundant;
    }

    /**
     * Mark this ResultColumn as redundant.
     */
    public void setRedundant()
    {
        isRedundant = true;
    }

    /**
     * Mark this ResultColumn as a grouping column in the SELECT list
     */
    public void markAsGroupingColumn()
    {
        isGroupingColumn = true;
    }

    public void markAsPulledupOrderingColumn() { isPulledupOrderingColumn = true; }

    public boolean pulledupOrderingColumn() { return isPulledupOrderingColumn; }

    /**
     * Look for and reject ?/-?/+? parameter under this ResultColumn.  This is
     * called for SELECT statements.
     *
     * @exception StandardException        Thrown if a ?/-?/+? parameter was found
     *                                    directly under this ResultColumn.
     */

    void rejectParameter() throws StandardException
    {
        if ((expression != null) && (expression.isParameterNode()))
            throw StandardException.newException(SQLState.LANG_PARAM_IN_SELECT_LIST);
    }

    /*
    ** The following methods implement the Comparable interface.
    */
    public int compareTo(Object other)
    {
        ResultColumn otherResultColumn = (ResultColumn) other;

        return this.getColumnPosition() - otherResultColumn.getColumnPosition();
    }

    /**
     * Mark this column as being updated by an update statemment.
     */
    void markUpdated()
    {
        updated = true;
    }

    /**
     * Mark this column as being updatable, so we can make sure it is in the
     * "for update" list of a positioned update.
     */
    void markUpdatableByCursor()
    {
        updatableByCursor = true;
    }

    /**
     * Tell whether this column is being updated.
     *
     * @return    true means this column is being updated.
     */
    boolean updated()
    {
        return updated;
    }

    /**
     * Tell whether this column is updatable by a positioned update.
     *
     * @return    true means this column is updatable
     */
    public boolean updatableByCursor()
    {
        return updatableByCursor;
    }

    /**
     * Make a copy of this ResultColumn in a new ResultColumn
     *
     * @return    A new ResultColumn with the same contents as this one
     */
    public ResultColumn cloneMe() throws StandardException
    {
        ResultColumn    newResultColumn;
        ValueNode        cloneExpr;

        /* If expression is a ColumnReference, then we want to
         * have the RC's clone have a clone of the ColumnReference
         * for it's expression.  This is for the special case of
         * cloning the SELECT list for the HAVING clause in the parser.
         * The SELECT generated for the HAVING needs its own copy
         * of the ColumnReferences.
         */
        if (expression instanceof ColumnReference || expression instanceof CurrentRowLocationNode)
        {
            cloneExpr = expression.getClone();
        }
        else
        {
            cloneExpr = expression;
        }

        /* If a columnDescriptor exists, then we must propagate it */
        if (columnDescriptor != null)
        {
            newResultColumn = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    columnDescriptor,
                    expression,
                    getContextManager());
            newResultColumn.setExpression(cloneExpr);
        }
        else
        {

            newResultColumn = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    getName(),
                    cloneExpr,
                    getContextManager());
        }

        /* Set the VirtualColumnId and name in the new node */
        newResultColumn.setVirtualColumnId(getVirtualColumnId());
        // Splice fork: also set the result set number on the new node
        newResultColumn.setResultSetNumber(getResultSetNumber());

        /* Set the type and name information in the new node */
        newResultColumn.setName(getName());
        newResultColumn.setType(getTypeServices());
        newResultColumn.setNameGenerated(isNameGenerated());

        // For OFFSET/FETCH we need the also clone table name to avoid failing
        // check #2 in EmbedResultSet#checksBeforeUpdateXXX. Clone schema for
        // good measure...
        newResultColumn.setSourceTableName(getSourceTableName());
        newResultColumn.setSourceSchemaName(getSourceSchemaName());

        /* Set the "is generated for unmatched column in insert" status in the new node
        This if for bug 4194*/
        if (isGeneratedForUnmatchedColumnInInsert())
            newResultColumn.markGeneratedForUnmatchedColumnInInsert();

        /* Set the "is referenced" status in the new node */
        if (isReferenced())
            newResultColumn.setReferenced();

        /* Set the "updated" status in the new node */
        if (updated())
            newResultColumn.markUpdated();

        /* Setthe "updatable by cursor" status in the new node */
        if (updatableByCursor())
            newResultColumn.markUpdatableByCursor();

        if (isAutoincrementGenerated())
            newResultColumn.setAutoincrementGenerated();

        if (isAutoincrement())
            newResultColumn.setAutoincrement();
        if (isGroupingColumn())
            newResultColumn.markAsGroupingColumn();

        if (isRightOuterJoinUsingClause()) {
            newResultColumn.setRightOuterJoinUsingClause(true);
        }

        if (getJoinResultSet() != null) {
            newResultColumn.setJoinResultset(getJoinResultSet());
        }

        if (isGenerated()) {
            newResultColumn.markGenerated();
        }
        if (pulledupOrderingColumn()) {
            newResultColumn.markAsPulledupOrderingColumn();
        }

        newResultColumn.fromLeftChild = this.fromLeftChild;
        return newResultColumn;
    }

    /**
     * Get the maximum size of the column
     *
     * @return the max size
     */
    public int getMaximumColumnSize()
    {
        return getTypeServices().getTypeId()
                .getApproximateLengthInBytes(getTypeServices());
    }

    public DataTypeDescriptor getTypeServices()
    {
        DataTypeDescriptor type = super.getTypeServices();
        if (type != null)
            return type;

        if (getExpression() != null)
            return getExpression().getTypeServices();

        return null;
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (method calls and non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *        CONSTANT                - constant
     *
     * @return    The variant type for the underlying expression.
     * @exception StandardException    thrown on error
     */
    protected int getOrderableVariantType() throws StandardException
    {
        /*
        ** If the expression is VARIANT, then
        ** return VARIANT.  Otherwise, we return
        ** CONSTANT. For result columns that are
        ** generating autoincrement values, the result
        ** is variant.
        */
        int expType;
        if (isAutoincrementGenerated()) {
            expType = Qualifier.VARIANT;
        } else if (expression != null) {
            expType = expression.getOrderableVariantType();
        } else {
            expType = Qualifier.CONSTANT;
        }

        switch (expType)
        {
            case Qualifier.VARIANT:
                return Qualifier.VARIANT;

            case Qualifier.SCAN_INVARIANT:
            case Qualifier.QUERY_INVARIANT:
                return Qualifier.SCAN_INVARIANT;

            default:
                return Qualifier.CONSTANT;
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (expression != null) {
            setExpression( (ValueNode)expression.accept(v, this));
        }
        if (v instanceof OffsetOrderVisitor && reference != null)
            reference = (ColumnReference)reference.accept(v, this);
    }

    /**
     * Is this column in this array of strings?
     *
     * @param list the array of column names to compare
     *
     * @return true/false
     */
    public boolean foundInList(String[] list)
    {
        return foundString(list, name);
    }

    /**
     * Verify that this RC is orderable.
     *
     * @exception StandardException        Thrown on error
     */
    void verifyOrderable() throws StandardException
    {
        /*
         * Do not check to see if we can map user types
         * to built-in types.  The ability to do so does
         * not mean that ordering will work.  In fact,
         * as of version 2.0, ordering does not work on
         * user types.
         */
        if (!getTypeId().orderable(getClassFactory()))
        {
            throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
                    getTypeId().getSQLTypeName());
        }
    }

    /**
     If this ResultColumn is bound to a column in a table
     get the column descriptor for the column in the table.
     Otherwise return null.
     */
    public ColumnDescriptor getTableColumnDescriptor() {return columnDescriptor;}

    /**
     * Returns true if this result column is a placeholder for a generated
     * autoincrement value.
     */
    public boolean isAutoincrementGenerated()
    {
        return autoincrementGenerated;
    }

    public void setAutoincrementGenerated()
    {
        autoincrementGenerated = true;
    }

    public void resetAutoincrementGenerated()
    {
        autoincrementGenerated = false;
    }

    public boolean isAutoincrement()
    {
        return autoincrement;
    }

    public void setAutoincrement()
    {
        autoincrement = true;
    }

    public boolean isGroupingColumn()
    {
        return isGroupingColumn;
    }

    /**
     * @exception StandardException        Thrown on error
     */
    private DataValueDescriptor convertConstant(TypeId toTypeId, int maxWidth,
                                                DataValueDescriptor constantValue)
            throws StandardException
    {
        int formatId = toTypeId.getTypeFormatId();
        DataValueFactory dvf = getDataValueFactory();
        switch (formatId)
        {
            default:
            case StoredFormatIds.CHAR_TYPE_ID:
                return constantValue;

            case StoredFormatIds.VARCHAR_TYPE_ID:
                String sourceValue = constantValue.getString();
                int sourceWidth = sourceValue.length();
                int posn;

                /*
                ** If the input is already the right length, no normalization is
                ** necessary - just return the source.
                **
                */

                if (sourceWidth <= maxWidth)
                {
                    if(formatId == StoredFormatIds.VARCHAR_TYPE_ID)
                        return dvf.getVarcharDataValue(sourceValue);
                }

                /*
                ** Check whether any non-blank characters will be truncated.
                */
                for (posn = maxWidth; posn < sourceWidth; posn++)
                {
                    if (sourceValue.charAt(posn) != ' ')
                    {
                        String typeName = null;
                        if (formatId == StoredFormatIds.VARCHAR_TYPE_ID)
                            typeName = TypeId.VARCHAR_NAME;
                        throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION,
                                typeName,
                                StringUtil.formatForPrint(sourceValue),
                                String.valueOf(maxWidth));
                    }
                }

                if (formatId == StoredFormatIds.VARCHAR_TYPE_ID)
                    return dvf.getVarcharDataValue(sourceValue.substring(0, maxWidth));

            case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                //No need to check widths here (unlike varchar), since no max width
                return dvf.getLongvarcharDataValue(constantValue.getString());

        }
    }

    public TableName getTableNameObject() {
        return null;
    }

    /* Get the wrapped reference if any */
    public    ColumnReference    getReference() { return reference; }

    /**
     * Get the source BaseColumnNode for this result column. The
     * BaseColumnNode cannot be found unless the ResultColumn is bound
     * and is a simple reference to a column in a BaseFromTable.
     *
     * @return a BaseColumnNode,
     *   or null if a BaseColumnNode cannot be found
     */
    public BaseColumnNode getBaseColumnNode() {
        ValueNode vn = expression;
        while (true) {
            if (vn instanceof ResultColumn) {
                vn = ((ResultColumn) vn).expression;
            } else if (vn instanceof ColumnReference) {
                vn = ((ColumnReference) vn).getSource();
            } else if (vn instanceof VirtualColumnNode) {
                vn = ((VirtualColumnNode) vn).getSourceColumn();
            } else if (vn instanceof BaseColumnNode) {
                return (BaseColumnNode) vn;
            } else {
                return null;
            }
        }
    }

    /**
     * Search the tree beneath this ResultColumn until we find
     * the number of the table to which this RC points, and
     * return that table number.  If we can't determine which
     * table this RC is for, then return -1.
     * <p>
     * There are two places we can find the table number: 1) if
     * our expression is a ColumnReference, then we can get the
     * target table number from the ColumnReference and that's
     * it; 2) if expression is a VirtualColumnNode, then if
     * the VirtualColumnNode points to a FromBaseTable, we can
     * get that FBT's table number; otherwise, we walk the
     * VirtualColumnNode-ResultColumn chain and do a recursive
     * search.
     *
     * @return The number of the table to which this ResultColumn
     * points, or -1 if we can't determine that from where we are.
     */
    @Override
    public int getTableNumber() {
        ResultColumn other = this;
        while (true) {
            if (other.expression instanceof ColumnReference)
                return ((ColumnReference) other.expression).getTableNumber();
            else if (other.expression instanceof VirtualColumnNode) {
                VirtualColumnNode vcn = (VirtualColumnNode) other.expression;

                // If the VCN points to a FromBaseTable, just get that
                // table's number.
                if (vcn.getSourceResultSet() instanceof FromBaseTable) {
                    return ((FromBaseTable) vcn.getSourceResultSet()).
                            getTableNumber();
                }

                // Else recurse down the VCN.
                other = vcn.getSourceColumn();
                continue;
            } else if (other.expression instanceof TernaryOperatorNode) {
                return ((TernaryOperatorNode) other.expression).receiver.getTableNumber();
            } else if (other.expression == null) {
                return -1;
            }

            // We can get here if expression has neither a column
            // reference nor a FromBaseTable beneath it--for example,
            // if it is of type BaseColumnNode.
            return other.expression.getTableNumber();
        }
    }

    public boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (o.getNodeType() == getNodeType())
        {
            ResultColumn other = (ResultColumn)o;
            if (expression != null) {
                return expression.isEquivalent(other.expression);
            }
        }
        return false;
    }

    public boolean equals(Object o){
        return this == o;
    }


    /**
     * Return true if this result column represents a generated column.
     */
    public boolean hasGenerationClause()
    {
        return (columnDescriptor != null) && columnDescriptor.hasGenerationClause();
    }

    public List<? extends QueryTreeNode> getChildren() {
        return Collections.singletonList(expression);
    }

    @Override
    public QueryTreeNode getChild(int index) {
        assert index == 0;
        return expression;
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        assert index == 0;
        expression = (ValueNode) newValue;
    }

    /**
     *
     * Utilizes statistics cardinality if available, if not it returns 0.
     *
     * @return
     * @throws StandardException
     */
    public long cardinality() throws StandardException {
        if (this.getTableColumnDescriptor() ==null) // Temporary JL
            return 0;
        ConglomerateDescriptor cd = this.getTableColumnDescriptor().getTableDescriptor().getConglomerateDescriptorList().get(0);
        int leftPosition = getColumnPosition();
        return getCompilerContext().getStoreCostController(this.getTableColumnDescriptor().getTableDescriptor(),cd, false, 0).cardinality(leftPosition);
    }
    /**
     *
     * Utilizes statistics cardinality if available, if not it returns the number of rows passed in.
     *
     * @return
     * @throws StandardException
     */
    @Override
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        long c = cardinality();
        return c> 0? c : expression.nonZeroCardinality(numberOfRows);
    }

    public ConglomerateDescriptor getBaseConglomerateDescriptor() {
        return getTableColumnDescriptor()==null?null:getTableColumnDescriptor().getBaseConglomerateDescriptor();
    }


    public void setColumnDescriptor(ColumnDescriptor columnDescriptor) {
        this.columnDescriptor = columnDescriptor;
    }

    /**
     * Returns a Spark Type for the column. This StructField
     * will not represent an actual value, it will only represent the Spark type
     * that all values in the column will have.
     *
     * @return    A StructField describing the type of the column.
     */
    public StructField getStructField() {
        try {
            return getType().getNull().getStructField(getName());
        } catch (StandardException e) {
            return null;
        }
    }

    public long getCoordinates() {
        return getExpression().getCoordinates(this);
    }

    public LongArrayList chain() {
        LongArrayList chain = new LongArrayList();
        ValueNode expression = getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                chain.add(sc.getCoordinates());
                expression = sc.getExpression();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null) { // A ColumnReference can be sourceless…
                    chain.add(sc.getCoordinates());
                    expression = sc.getExpression();
                } else {
                    expression = null;
                }
            } else if (expression instanceof CastNode) {
                expression = ((CastNode) expression).getCastOperand();
            }
            else {
                expression = null;
            }
        }

        return chain;
    }

    public long getChildLink() {
        ValueNode expression = getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                if (sc.getResultSetNumber() == -1)
                    return -1L;
                return sc.getCoordinates();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null) { // A ColumnReference can be sourceless…
                    return sc.getCoordinates();
                } else {
                    expression = null;
                }
            } else if (expression instanceof CastNode) {
                expression = ((CastNode) expression).getCastOperand();
            }
            else {
                expression = null;
            }
        }
        return -1L;
    }


    public void setFromLeftChild(boolean fromLeftChild) {
        this.fromLeftChild = fromLeftChild;
    }

    public boolean isFromLeftChild() {
        return fromLeftChild;
    }
}

