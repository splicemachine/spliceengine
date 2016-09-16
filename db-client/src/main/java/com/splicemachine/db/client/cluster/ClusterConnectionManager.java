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

import com.splicemachine.db.iapi.reference.SQLState;

import java.sql.*;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Properties;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
class ClusterConnectionManager{
    private final ClusteredConnection sourceConn;
    private Properties connectionProperties;
    private final ClusteredDataSource poolSource;
    private RefCountedConnection currentConn;

    private final Set<ClusteredStatement> openStatements =Collections.newSetFromMap(new IdentityHashMap<ClusteredStatement, Boolean>());

    ClusterConnectionManager(ClusteredConnection sourceConn,
                             ClusteredDataSource poolSource,
                             Properties connectionProperties){
        this.poolSource=poolSource;
        this.sourceConn = sourceConn;
        this.connectionProperties=connectionProperties;
    }

    private boolean autoCommit = true;
    private boolean queryRan = false; //only set if autocommit = false

    private boolean readOnly = false;
    private TxnIsolation isolationLevel = TxnIsolation.READ_COMMITTED;
    private int holdability =ResultSet.CLOSE_CURSORS_AT_COMMIT;
    private String schema;

    public void setAutoCommit(boolean autoCommit) throws SQLException{
        if(hasOpenConnection()){
            if(this.autoCommit!=autoCommit)
                currentConn.element().setAutoCommit(autoCommit); //force the transaction commit
        }
        this.autoCommit = autoCommit;
    }

    public void commit() throws SQLException{
        if(hasOpenConnection()){
            currentConn.element().commit();
            releaseConnectionIfPossible();
        }
        queryRan=false;
    }

    public void rollback() throws SQLException{
        if(hasOpenConnection()){
            currentConn.element().rollback();
            releaseConnectionIfPossible();
        }
        queryRan = false;
    }

    public void close(boolean closeDataSourceOnClose) throws SQLException{
        if(openStatements.size()>0){
            throw new SQLException("There are open statements",SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION);
        }
        if(hasOpenConnection())
            currentConn.element().close(); //this really just returns the connection to the pool, but close enough
        if(closeDataSourceOnClose)
            this.poolSource.close();
    }

    public void setReadOnly(boolean readOnly) throws SQLException{
        if(currentConn!=null && this.readOnly !=readOnly)
            currentConn.element().setReadOnly(readOnly);
    }

    public boolean isReadOnly(){
        return readOnly;
    }

    public void setHoldability(int holdability) throws SQLException{
        this.holdability = holdability;
        if(hasOpenConnection())
            currentConn.element().setHoldability(holdability);
    }

    public int getHoldability(){
        return holdability;
    }

    public boolean isValid(int timeout) throws SQLException{
        reopenConnectionIfNecessary();
        return currentConn.element().isValid(timeout);
    }

    public void setSchema(String schema) throws SQLException{
        this.schema = schema;
        if(hasOpenConnection())
            currentConn.element().setSchema(schema);

    }

    public String getSchema(){
        return schema;
    }

    public boolean isAutoCommit(){
        return autoCommit;
    }

    public Statement createStatement(int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        reopenConnectionIfNecessary();
        Statement delegate = currentConn.element().createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
        ClusteredStatement cs = new ClusteredStatement(currentConn,sourceConn,delegate);
        registerStatement(cs);
        queryRan = true;

        return cs;
    }

    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        reopenConnectionIfNecessary();
        PreparedStatement delegate = currentConn.element().prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
        ClusteredPreparedStatement cps = new ClusteredPreparedStatement(currentConn,sourceConn,delegate);
        registerStatement(cps);
        queryRan=true;

        return cps;
    }

    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        reopenConnectionIfNecessary();
        CallableStatement delegate = currentConn.element().prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
        ClusteredCallableStatement cps = new ClusteredCallableStatement(currentConn,sourceConn,delegate);
        registerStatement(cps);
        queryRan=true;

        return cps;
    }

    public DatabaseMetaData getMetaData() throws SQLException{
        reopenConnectionIfNecessary();
        return currentConn.element().getMetaData();
    }

    TxnIsolation getTxnIsolation(){
        return isolationLevel;
    }

    void setTxnIsolation(TxnIsolation isolationLevel) throws SQLException{
        this.isolationLevel = isolationLevel;
        if(hasOpenConnection())
            currentConn.element().setTransactionIsolation(isolationLevel.level);
    }

    boolean inActiveTransaction(){
        return currentConn!=null && !autoCommit && queryRan;
    }

    private void registerStatement(ClusteredStatement cs){
        currentConn.acquire();
        openStatements.add(cs);
    }

    void releaseStatement(ClusteredStatement clusteredStatement){
        openStatements.remove(clusteredStatement);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void reopenConnectionIfNecessary() throws SQLException{
        if(hasOpenConnection()) return;

        Connection conn = this.poolSource.getConnection(); //TODO -sf- do username/password here
        conn.setAutoCommit(autoCommit);
        conn.setTransactionIsolation(isolationLevel.level);
        conn.setHoldability(holdability);

        if(schema!=null)
            conn.setSchema(schema);

        currentConn = new RefCountedConnection(conn);
    }

    private boolean hasOpenConnection() throws SQLException{
        return currentConn!=null &&!currentConn.element().isClosed();
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
        if(currentConn.getReferenceCount()<=0){
            currentConn.element().close(); //release the underlying connection to the pool
            currentConn=null; //null the reference so that we know to reconnect
        }else{
           currentConn.invalidate();
        }
    }

}
