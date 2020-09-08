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
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Iterator;

import com.splicemachine.db.client.am.stmtcache.JDBCStatementCache;
import com.splicemachine.db.client.am.stmtcache.StatementKey;
import com.splicemachine.db.client.am.stmtcache.StatementKeyFactory;
import com.splicemachine.db.jdbc.ClientDriver;
import com.splicemachine.db.shared.common.sanity.SanityManager;

/**
 * Utility class encapsulating the logic for interacting with the JDBC statement
 * cache when creating new logical statements.
 * <p>
 * This class was introduced to share code between the pre-JDBC 4 and the JDBC
 * 4+ versions of the JDBC classes.
 * <p>
 * The pattern for the {@code prepareX} methods is:
 * <ol> <li>Generate a key for the statement to create.</li>
 *      <li>Consult cache to see if an existing statement can be used.</li>
 *      <li>Create new statement on physical connection if necessary.</li>
 *      <li>Return reference to existing or newly created statement.</li>
 * </ol>
 */
public final class StatementCacheInteractor {

    /** Statement cache for the associated physical connection. */
    private final JDBCStatementCache cache;
    /**
     * The underlying physical connection.
     * <p>
     * Note that it is the responsibility of the logical statement assoiciated
     * with this cache interactor to ensure the interactor methods are not
     * invoked if the logical statement has been closed.
     */
    private final ClientConnection physicalConnection;
    /** List of open logical statements created by this cache interactor. */
    //@GuardedBy("this")
    private final ArrayList openLogicalStatements = new ArrayList();
    /**
     * Tells if this interactor is in the process of shutting down.
     * <p>
     * If this is true, it means that the logical connection is being closed.
     */
    private boolean connCloseInProgress = false;

    /**
     * Creates a new JDBC statement cache interactor.
     *
     * @param cache statement cache
     * @param physicalConnection associated physical connection
     */
    StatementCacheInteractor(JDBCStatementCache cache,
                             ClientConnection physicalConnection) {
        this.cache = cache;
        this.physicalConnection = physicalConnection;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String)
     */
    public synchronized PreparedStatement prepareStatement(String sql)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.holdability());
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(sql);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int,int)
     */
    public synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int resultSetType,
                                                int resultSetConcurrency)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, physicalConnection.holdability());
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(
                    sql, resultSetType, resultSetConcurrency);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int,int,int)
     */
    public synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int resultSetType,
                                                int resultSetConcurrency,
                                                int resultSetHoldability)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, resultSetHoldability);

        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(
                sql, resultSetType,resultSetConcurrency, resultSetHoldability);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String,int)
     */
    public synchronized PreparedStatement prepareStatement(
                                                String sql,
                                                int autoGeneratedKeys)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newPrepared(
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.getHoldability(), autoGeneratedKeys);
        PreparedStatement ps = cache.getCached(stmtKey);
        if (ps == null) {
            ps = physicalConnection.prepareStatement(sql, autoGeneratedKeys);
        }
        return createLogicalPreparedStatement(ps, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String)
     */
    public synchronized CallableStatement prepareCall(String sql)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
                sql, physicalConnection.getCurrentSchemaName(),
                physicalConnection.holdability());
        CallableStatement cs = (CallableStatement)cache.getCached(stmtKey);
        if (cs == null) {
            cs = physicalConnection.prepareCall(sql);
        }
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String,int,int)
     */
    public synchronized CallableStatement prepareCall(String sql,
                                                      int resultSetType,
                                                      int resultSetConcurrency)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, physicalConnection.holdability());
        CallableStatement cs = (CallableStatement)cache.getCached(stmtKey);
        if (cs == null) {
            cs = physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        }
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * @see java.sql.Connection#prepareCall(String,int,int,int)
     */
    public synchronized CallableStatement prepareCall(String sql,
                                                      int resultSetType,
                                                      int resultSetConcurrency,
                                                      int resultSetHoldability)
            throws SQLException {
        StatementKey stmtKey = StatementKeyFactory.newCallable(
                sql, physicalConnection.getCurrentSchemaName(), resultSetType,
                resultSetConcurrency, resultSetHoldability);
        CallableStatement cs = (CallableStatement)cache.getCached(stmtKey);
        if (cs == null) {
            cs = physicalConnection.prepareCall(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
        return createLogicalCallableStatement(cs, stmtKey);
    }

    /**
     * Closes all open logical statements created by this cache interactor.
     * <p>
     * A cache interactor is bound to a single (caching) logical connection.
     * @throws SQLException if closing an open logical connection fails
     */
    public synchronized void closeOpenLogicalStatements()
            throws SQLException {
        // Transist to closing state, to avoid changing the list of open
        // statements as we work our way through the list.
        this.connCloseInProgress = true;
        // Iterate through the list and close the logical statements.
        Iterator logicalStatements = this.openLogicalStatements.iterator();
        while (logicalStatements.hasNext()) {
            LogicalStatementEntity logicalStatement =
                    (LogicalStatementEntity)logicalStatements.next();
            logicalStatement.close();
        }
        // Clear the list for good measure.
        this.openLogicalStatements.clear();
    }

    /**
     * Designates the specified logical statement as closed.
     *
     * @param logicalStmt the logical statement being closed
     */
    public synchronized void markClosed(LogicalStatementEntity logicalStmt) {
        // If we are not in the process of shutting down the logical connection,
        // remove the notifying statement from the list of open statements.
        if (!connCloseInProgress) {
            boolean removed = this.openLogicalStatements.remove(logicalStmt);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(removed,
                    "Tried to remove unregistered logical statement: " +
                    logicalStmt);
            }
        }
    }

    /**
     * Creates a logical prepared statement.
     *
     * @param ps the underlying physical prepared statement
     * @param stmtKey the statement key for the physical statement
     * @return A logical prepared statement.
     * @throws SQLException if creating a logical prepared statement fails
     */
    private PreparedStatement createLogicalPreparedStatement(
                                                    PreparedStatement ps,
                                                    StatementKey stmtKey)
            throws SQLException {
        LogicalPreparedStatement logicalPs =
                ClientDriver.getFactory().newLogicalPreparedStatement(
                                                    ps, stmtKey, this);
       this.openLogicalStatements.add(logicalPs);
       return logicalPs;
    }

    /**
     * Creates a logical callable statement.
     *
     * @param cs the underlying physical callable statement
     * @param stmtKey the statement key for the physical statement
     * @return A logical callable statement.
     * @throws SQLException if creating a logical callable statement fails
     */
    private CallableStatement createLogicalCallableStatement(
                                                    CallableStatement cs,
                                                    StatementKey stmtKey)
            throws SQLException {
        LogicalCallableStatement logicalCs =
                ClientDriver.getFactory().newLogicalCallableStatement(
                                                    cs, stmtKey, this);
       this.openLogicalStatements.add(logicalCs);
       return logicalCs;
    }

    /**
     * Returns the associated statement cache.
     *
     * @return A statement cache.
     */
    JDBCStatementCache getCache() {
        return this.cache;
    }
}
