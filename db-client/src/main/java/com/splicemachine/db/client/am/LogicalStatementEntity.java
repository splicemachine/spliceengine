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
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
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

import java.sql.SQLException;

import com.splicemachine.db.client.am.stmtcache.JDBCStatementCache;
import com.splicemachine.db.client.am.stmtcache.StatementKey;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.shared.common.sanity.SanityManager;

/**
 * Common class interacting with the JDBC statement cache for logical prepared
 * statements and logical callable statements.
 * <p>
 * Note that {@link #getPhysPs} and {@link #getPhysCs} takes care of checking if
 * the logical statement has been closed. The physical statement will take care
 * of validating itself.
 * <p>
 * Beside from the above, special treatment of logical entities happens
 * on close. This is the point where cache interaction takes place, and also
 * where the appropriate methods are called on the physical statement to perform
 * the necessary clean up for later reuse.
 * <p>
 * A note regarding the thread safety of this class, is that access to
 * {@code physicalPs} and {@code physicalCs} is guarded by the instance of this
 * class, but it is assumed that operation on/within the physical statement is
 * synchronized in the physical statement itself .
 */
//@ThreadSafe
abstract class LogicalStatementEntity
        implements java.sql.Statement {

    /**
     * Tells if we're holding a callable statement or not.
     * <p>
     * Used for sanity checking.
     */
    private final boolean hasCallableStmt;
    /**
     * Associated physical prepared statement.
     * <p>
     * If this is {@code null}, the logical entity is closed.
     */
    //@GuardedBy("this")
    private java.sql.PreparedStatement physicalPs;
    /**
     * Assoicated physical callable statement, if any.
     * <p>
     * This is a convenience reference, to avoid having to cast on every
     * invokation of {@link #getPhysCs} if the logical entity represents a
     * callable statement.
     */
    //@GuardedBy("this)
    private java.sql.CallableStatement physicalCs;
    /** The owner of this logical entity. */
    private StatementCacheInteractor owner;
    /** The key for the associated statement. */
    private final StatementKey stmtKey;
    /** Cache for physical statements. */
    //@GuardedBy("this)
    private final JDBCStatementCache cache;

    /**
     * Create a logical entity for a {@link java.sql.PreparedStatement}.
     *
     * @param physicalPs a physical {@link java.sql.PreparedStatement}
     * @param stmtKey cache key for the physical statement
     * @param cacheInteractor creating statement cache interactor
     * @throws IllegalArgumentException if {@code cache} is {@code null}
     */
    protected LogicalStatementEntity(java.sql.PreparedStatement physicalPs,
                                     StatementKey stmtKey,
                                     StatementCacheInteractor cacheInteractor) {
        if (cacheInteractor.getCache() == null) {
            // Internal check, failure indicates programming error.
            // No need to localize error message.
            throw new IllegalArgumentException(
                    "statement cache reference cannot be <null>");
        }
        this.stmtKey = stmtKey;
        this.cache = cacheInteractor.getCache();
        this.owner = cacheInteractor;
        this.physicalPs = physicalPs;
        if (physicalPs instanceof java.sql.CallableStatement) {
            this.hasCallableStmt = true;
            this.physicalCs = (java.sql.CallableStatement)physicalPs;
        } else {
            this.hasCallableStmt = false;
            this.physicalCs = null;
        }
        ((PreparedStatement)physicalPs).setOwner(this);
    }

    /**
     * Returns the associated physical prepared statement.
     *
     * @return A prepared statement.
     * @throws SQLException if the logical statement has been closed
     */
    synchronized java.sql.PreparedStatement getPhysPs()
            throws SQLException {
        if (physicalPs == null) {
            throw (new SqlException(null,
                new ClientMessageId(SQLState.ALREADY_CLOSED),
                                    "PreparedStatement")).getSQLException();
        }
        return physicalPs;
    }

    /**
     * Returns the associated physical callable statement.
     *
     * @return A callable statement.
     * @throws SQLException if the logical statement has been closed
     */
    synchronized java.sql.CallableStatement getPhysCs()
            throws SQLException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(hasCallableStmt,
                    "called getPhysCs(), but created with PreparedStatement");
        }
        if (physicalCs == null) {
            throw (new SqlException(null,
                new ClientMessageId(SQLState.ALREADY_CLOSED),
                                    "CallableStatement")).getSQLException();
        }
        return physicalCs;
    }

    /**
     * Returns the associated physical statement.
     *
     * @return A statement.
     * @throws SQLException if the logical statement has been closed
     */
    synchronized java.sql.Statement getPhysStmt()
            throws SQLException
    {
        if ( hasCallableStmt ) { return getPhysCs(); }
        else { return getPhysPs(); }
    }

    /**
     * Close the logical statement.
     *
     * @throws SQLException if closing the statement fails
     */
    public synchronized void close() throws SQLException {
        if (physicalPs != null) {
            final PreparedStatement temporaryPsRef =
                    (com.splicemachine.db.client.am.PreparedStatement)physicalPs;
            // Nullify both references.
            physicalPs = null;
            physicalCs = null;

            this.owner.markClosed(this);
            // Nullify the reference, since the entity object might stick around
            // for a while.
            this.owner = null;
            // Reset the owner of the physical statement.
            temporaryPsRef.setOwner(null);
            // NOTE: Accessing ps state directly below only to avoid tracing.
            // If the underlying statement has become closed, don't cache it.
            if (!temporaryPsRef.openOnClient_) {
                return;
            }
            // If the poolable hint is false, don't cache it.
            if (!temporaryPsRef.isPoolable) {
                temporaryPsRef.close();
                return;
            }

            // Reset the statement for reuse.
            try {
                temporaryPsRef.resetForReuse();
            } catch (SqlException sqle) {
                // Get a wrapper and throw it.
                throw sqle.getSQLException();
            }

            // Try to insert the statement into the cache.
            if (!cache.cacheStatement(stmtKey, temporaryPsRef)) {
                // Statement was already in the cache, discard this one.
                temporaryPsRef.close();
            }
        }
    }

    /**
     * Tells if the logical entity is closed.
     * <p>
     * If this method is used to avoid the possibility of raising an exception
     * because the logical statement has been closed and then invoke a method on
     * the physical statement, one must synchronize on this instance in the
     * calling code.
     *
     * @return {@code true} if closed, {@code false} if open.
     */
    synchronized boolean isLogicalEntityClosed() {
        return (physicalPs == null);
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  void    closeOnCompletion() throws SQLException
    {
        ((com.splicemachine.db.client.am.Statement) getPhysStmt()).closeOnCompletion();
    }

    public  boolean isCloseOnCompletion() throws SQLException
    {
        return ((com.splicemachine.db.client.am.Statement) getPhysStmt()).isCloseOnCompletion();
    }
}
