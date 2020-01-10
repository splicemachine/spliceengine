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

package com.splicemachine.db.vti;

import java.sql.SQLException;

/**
 * This interface is implemented by a read/write VTI class that wants to control when
 * modifications to the VTI are deferred, or to be notified that a it is to be modified.
 * Consider the following statement:<br>
 * UPDATE NEW myVTI(...)
 *  SET cost = cost + 10
 *  WHERE cost < 15
 *<p>
 * Updating a column that is used in the WHERE clause might or might not give the VTI implementation trouble;
 * the update might cause the same row to be selected more than once. This problem can be solved by building the
 * complete list of rows to be updated and the new column values before updating any rows. The updates are applied
 * after the list is built. This process is called "deferred update".
 *<p>
 * By default, updates on a VTI are deferred when the VTI ResultSet
 * is scrollable (ResultSet.TYPE_SCROLL_SENSITIVE or TYPE_SCROLL_INSENSITIVE), and one or more of the following is true.
 *<ol>
 *<li>One or more of the columns in the SET clause is also used in the WHERE
 * clause and the VTI ResultSet is sensitive. We do not defer updates
 * when the ResultSet is TYPE_SCROLL_INSENSITIVE because it is not necessary.
 *<li>The where clause contains a subselect on a VTI from the same class as the
 * target VTI. We do not look at the VTI parameters, just the VTI class name.
 *</ol>
 *<p>
 * By default, deletes on a VTI are deferred in a similar situation: when the VTI ResultSet
 * is scrollable (ResultSet.TYPE_SCROLL_SENSITIVE or TYPE_SCROLL_INSENSITIVE), and
 * the where clause contains a subselect on a VTI from the same class as the
 * target VTI. We do not look at the VTI parameters, just the VTI class name.
 *<p>
 * By default, inserts into a VTI are deferred when the same VTI class is used as both
 * the source and target. It does not depend on the scrollability of the
 * VTI ResultSet because inserts can be deferred without scrolling the ResultSet.
 *<p>
 * If these defaults are not appropriate then the class implementing the VTI should also implement
 * this interface (com.splicemachine.db.vti.DeferModification).
 *<p>
 * (A read/write VTI is implemented by a class that implements the java.sql.PreparedStatement interface,
 * often by extending the UpdatableVTITemplate interface. @see UpdatableVTITemplate).
 *<p>
 * Update and delete statement deferral is implemented by scrolling the VTI's ResultSet. Therefore,
 * updates and deletes on a VTI are never deferred unless the VTI's ResultSets are scrollable, even
 * if the DeferModification interface methods return <b>true</b>.
 * Therefore for an update or delete to be deferred the VTI getResultSetType() method must return
 * ResultSet.TYPE_SCROLL_SENSITIVE or TYPE_SCROLL_INSENSITIVE and the VTI must produce scrollable
 * java.sql.ResultSets that implement the getRow() and absolute() methods. If your VTI is implemented as
 * an extension to UpdatableVTITemplate then you must override the getResultSetMethod:
 * UpdatableVTITemplate.getResultSetType()
 * throws an exception. If your VTI's ResultSets are implemented as extensions to VTITemplate then you must
 * override the getRow() and absolute() methods: VTITemplate.getRow() and absolute() throw exceptions.
 *<p>
 * This interface is not used when the VTI is referenced only in a subselect; it is only used when a
 * VTI appears as the target of an INSERT, UPDATE, or DELETE statement.
 */
public interface DeferModification
{

    int INSERT_STATEMENT = 1;
    int UPDATE_STATEMENT = 2;
    int DELETE_STATEMENT = 3;

    /**
     * This method is called during preparation of an insert, update, or delete statement with this VTI
     * as the target. It indicates whether the statement should be deferred irregardless of the other clauses
     * in the statement. If alwaysDefer returns <b>true</b> then the other methods in this interface
     * are not called. (At least not for this statement type).
     *
     * @param statementType One of INSERT_STATEMENT, UPDATE_STATEMENT, DELETE_STATEMENT.
     *
     * @return <b>true</b> if the statement type should always be deferred on this VTI,
     *         <b>false</b> other criteria (see below) should be examined to determine
     *         whether to defer the modification.
     *
     * @exception SQLException on an unexpected condition.
     */
    boolean alwaysDefer(int statementType)
        throws SQLException;

    /**
     * This method is called during preparation of an update or delete statement on the virtual
     * table if getResultSetType() returns ResultSet.TYPE_SCROLL_SENSITIVE or TYPE_SCROLL_SENSITIVE and
     * alwaysDefer( statementType) returns <b>false</b>.
     * ColumnRequiresDefer is called once for each column that is being updated,
     * or each column in a DELETE where clause until
     * it returns <b>true</b> or until all the columns have been exhausted.
     *
     * @param statementType UPDATE_STATEMENT or DELETE_STATEMENT.
     * @param columnName the name of one of the columns being updated
     * @param inWhereClause indicates whether the column also appears in the where clause
     *
     * @return <b>true</b> if the update must be deferred
     *         <b>false</b> if this column does not require a deferred update
     *
     * @exception SQLException a parameter is invalid or there is another unexpected failure.
     */
    boolean columnRequiresDefer(int statementType,
                                String columnName,
                                boolean inWhereClause)
        throws SQLException;

    /**
     * This method is called during preparation of an insert, update, or delete statement that has this virtual
     * table as its target and that has a sub-select. It is invoked once for each regular table in a sub-select,
     * if it has not already been determined that the statement should be deferred or that the VTI does not support
     * deferral.
     *
     * @param statementType the statement type: INSERT_STATEMENT, UPDATE_STATEMENT, or DELETE_STATEMENT.
     * @param schemaName the schema of the table in the sub-select.
     * @param tableName the name of the table in the sub-select.
     *
     * @return <b>true</b> if the modification must be deferred
     *         <b>false</b> if this source table does not necessitate a deferred modification
     *
     * @exception SQLException a parameter is invalid or there is another unexpected failure.
     */
    boolean subselectRequiresDefer(int statementType,
                                   String schemaName,
                                   String tableName)
        throws SQLException;

    /**
     * This method is called during preparation of an insert, update, or delete statement that has this virtual
     * table as its target and that has a sub-select. It is invoked once for each virtual table in the sub-select,
     * if it has not already been determined that the statement should be deferred or that the VTI does not support
     * deferral.
     *
     * @param statementType the statement type: INSERT_STATEMENT, UPDATE_STATEMENT, or DELETE_STATEMENT.
     * @param VTIClassName the name of the class implementing the VTI in the sub-select.
     *
     * @return <b>true</b> if the modification must be deferred
     *         <b>false</b> if this source table does not necessitate a deferred modification
     *
     * @exception SQLException a parameter is invalid or there is another unexpected failure.
     */
    boolean subselectRequiresDefer(int statementType,
                                   String VTIClassName)
        throws SQLException;

    /**
     * This VTI method is called by Derby when a VTI modification (insert, update, or delete)
     * is executed. It is called after the VTI has been instantiated but before any rows are read,
     * inserted, updated, or deleted.
     *
     * @param statementType one of INSERT_STATEMENT, UPDATE_STATEMENT, or DELETE_STATEMENT
     * @param deferred <b>true</b> if the modification will be deferred, <b>false</b> if not.
     *
     * @exception SQLException thrown on an unexpected failure
     */
    void modificationNotify(int statementType,
                            boolean deferred)
        throws SQLException;
}
