/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.client.am.ResultSet;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Scott Fines
 *         Date: 11/15/16
 */
class ClusteredConnManager implements Debuggable{
    private static final int DEFAULT_EXECUTION_RETRY = 3;
    private static final Logger LOGGER=Logger.getLogger(ClusteredConnManager.class.getName());

    /*The DataSource from which to acquire connections*/
    private final ClusteredDataSource poolSource;
    /*A Registry of open statements*/
    private final Set<ClusteredStatement> openStatements =Collections.newSetFromMap(new IdentityHashMap<ClusteredStatement, Boolean>());
    /*The maximum number of times to retry an operation*/
    private final int executionRetryCount;

    private enum State{
        ACTIVE, //A connection has been acquired, and is actively in use by a statement
        INACTIVE, //No connection has been acquired (or all connections have been released)
        INACTIVE_COMMITTED, //no connection is acquired, but commit() has been called
        INACTIVE_ROLLED_BACK, //no connection is acquired, but rollback() has been called
        CLOSED, //the conn manager has been closed
    }

    /*
     * The state of the manager. Each state allows specific behavior
     */
    private State currState = State.INACTIVE;

    /*Whether or not connections should be treated as auto-commit or not*/
    private boolean autoCommit = true;
    /*whether or not to treat connections as read-only*/
    private boolean readOnly = false;

    /*The holdability. To preserve Derby behavior, we use HOLD_CURSORS_OVER_COMMIT as our default value.*/
    private int holdability =ResultSet.HOLD_CURSORS_OVER_COMMIT;

    /*The isolation to use. We default to READ_COMMITTED to preserve Derby expectations*/
    private TxnIsolation isolationLevel = TxnIsolation.READ_COMMITTED;

    /*The schema to use for connections*/
    private String schema;

    /*
     * The currently held connection.
     *
     * If the currState is INACTIVE,INACTIVE_COMMITTED,or INACTIVE_ROLLEDBACK, then
     * this will be null.
     *
     * If the currState is ACTIVE, then this will be non-null, and have a non-zero reference
     * count.
     *
     * If the currState is CLOSED, this will be null.
     */
    private RefCountedConnection currentConnection;

    ClusteredConnManager(ClusteredDataSource poolSource){
        this(poolSource,DEFAULT_EXECUTION_RETRY);
    }

    ClusteredConnManager(ClusteredDataSource poolSource,int executionRetryCount){
        this.poolSource=poolSource;
        this.executionRetryCount=executionRetryCount;
    }

    @Override
    public void logDebugInfo(Level logLevel){
        if(!LOGGER.isLoggable(logLevel)) return;

        String debugString = "{\n"
                + "currState: "+ currState+",\n"
                + "autoCommit: "+ autoCommit+",\n"
                + "readOnly: "+readOnly+",\n"
                + "holdability: "+holdability+",\n"
                + "isolation: "+ isolationLevel+",\n"
                + "schema: "+ schema+",\n"
                + "currConn: "+currentConnection+",\n"
                + "openStatementCount: "+openStatements.size()+"\n"
                + "}";
        LOGGER.log(logLevel,debugString);
        poolSource.logDebugInfo(logLevel);
    }

    /*Property management methods*/
    public void setAutoCommit(boolean autoCommit) throws SQLException{
        if(this.autoCommit!=autoCommit){
            this.autoCommit = autoCommit;
            if(currState == State.ACTIVE)
                currentConnection.element().setAutoCommit(autoCommit);
        }
    }

    public boolean isAutoCommit(){ return autoCommit;}

    public void setReadOnly(boolean readOnly) throws SQLException{
        if(this.readOnly !=readOnly){
            this.readOnly = readOnly;
            if(currState == State.ACTIVE)
                currentConnection.element().setReadOnly(readOnly);
        }
    }

    public boolean isReadOnly(){ return readOnly;}

    public void setHoldability(int holdability) throws SQLException{
        if(this.holdability !=holdability){
            this.holdability = holdability;
            if(currState==State.ACTIVE)
                currentConnection.element().setHoldability(holdability);
        }
    }

    public int getHoldability(){ return holdability;}

    public void setSchema(String schema) throws SQLException{
        this.schema = schema;
        if(currState==State.ACTIVE)
            currentConnection.element().setSchema(schema);
    }

    public String getSchema() throws SQLException{
        if(schema==null){
            activateConnectionIfNecessary();
            schema = currentConnection.element().getSchema();
        }
        return schema;
    }

    public void setIsolationLevel(TxnIsolation isolation) throws SQLException{
        this.isolationLevel = isolation;
        if(currState==State.ACTIVE)
            currentConnection.element().setTransactionIsolation(isolation.level);
    }

    public TxnIsolation getIsolationLevel() {return isolationLevel;}

    boolean inActiveTransaction(){
        return currState ==State.ACTIVE && !autoCommit;
    }

    public boolean validate(int timeout) throws SQLException{
        activateConnectionIfNecessary();
        return currentConnection.element().isValid(timeout);
    }


    int maxExecutionRetry(){ return executionRetryCount;}

    /*State management methods*/
    void registerStatement(ClusteredStatement cs){
        openStatements.add(cs);
    }

    void releaseStatement(ClusteredStatement cs){
        openStatements.remove(cs);
    }

    RefCountedConnection acquireConnection() throws SQLException{
        activateConnectionIfNecessary();
        return currentConnection;
    }

    RefCountedConnection forceReacquireConnection() throws SQLException{
        releaseConnectionIfPossible();
        return acquireConnection();
    }

    public void close(boolean closeDataSourceOnClose) throws SQLException{
        if(openStatements.size()>0){
            throw new SQLException("There are open statements",SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION);
        }
        if(currState==State.ACTIVE){
            Connection element=currentConnection.element();
            if(!autoCommit)
                element.rollback();
            element.close(); //this really just returns the connection to the pool, but close enough
        }
        if(closeDataSourceOnClose)
            this.poolSource.close();
        currState = State.CLOSED;
    }

    /*Transaction methods*/
    public void commit() throws SQLException{
        switch(currState){
            case ACTIVE:
                currentConnection.element().commit();
                releaseConnectionIfPossible();
                break;
            case INACTIVE:
                currState = State.INACTIVE_COMMITTED;
                break;
            case INACTIVE_COMMITTED:
            case INACTIVE_ROLLED_BACK: //do nothing if we're already in an inactive-terminal state
                break;
            default:
                throw new IllegalStateException("Cannot commit a closed manager!");
        }
    }


    public void rollback() throws SQLException{
        switch(currState){
            case ACTIVE:
                currentConnection.element().rollback();
                releaseConnectionIfPossible();
                break;
            case INACTIVE:
                currState = State.INACTIVE_ROLLED_BACK;
                break;
            case INACTIVE_COMMITTED:
            case INACTIVE_ROLLED_BACK: //do nothing if we're already in an inactive-terminal state
                break;
            default:
                throw new IllegalStateException("Cannot rollback a closed manager!");
        }
    }

    /* **********************************************************************/
    /*private helper methods*/
    private void activateConnectionIfNecessary() throws SQLException{
        switch(currState){
            case ACTIVE:
                return;
            case CLOSED:
                throw new IllegalStateException("Manager closed!");
        }
        assert currentConnection==null: "Programmer error: State is INACTIVE, but currentConnection!=null";

        reopen();
        currState = State.ACTIVE;
    }

    private void reopen() throws SQLException{
        Connection conn = this.poolSource.getConnection();
        conn.setAutoCommit(autoCommit);
        conn.setTransactionIsolation(isolationLevel.level);
        conn.setHoldability(holdability);
        conn.setReadOnly(readOnly);

        if(schema!=null) conn.setSchema(schema);
        switch(currState){
            case INACTIVE_COMMITTED:
                conn.commit();
                break;
            case INACTIVE_ROLLED_BACK:
                conn.rollback();
                break;
        }

        currentConnection = new RefCountedConnection(conn);
    }

    private void releaseConnectionIfPossible() throws SQLException{
        /*
         * If we do not have open statements, then we just close this connection
         * and allow it to return to the pool directly.
         *
         * However, there may be open statements when this method is called:
         *
         * try(Statement s = conn.createStatement){
         *  conn.commit()
         *  //do stuff with statement
         * }
         *
         * When this happens, the commit() will trigger a release of this connection,
         * but the open statement must remain connected to the existing server. We do
         * this by passing the connection ref to the statement, and then nulling out
         * our own internal reference. This allows existing statements to work
         * against the correct connection, but allows newly created statements to operate
         * against a new server (and therefore load balancing).
         *
         * The downside is that this allows Connections to leak from the pool if people don't
         * close their statements properly--so close your statements properly!
         */
        if(currentConnection.getReferenceCount()<=0)
            currentConnection.element().close();
        else
            currentConnection.invalidate();

        currentConnection=null; //null the reference so that we know to reconnect
        currState = State.INACTIVE;
    }
}
