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

import javax.sql.DataSource;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A Pool of Connections to a specific server.
 *
 * @author Scott Fines
 *         Date: 8/15/16
 */
class ServerPool{
    /*
     * We use java.util.logging here to avoid requiring a logging jar dependency on our applications (and therefore
     * causing all kinds of potential dependency problems).
     */
    private static final Logger LOGGER = Logger.getLogger(ClusteredDataSource.class.getName());

    final String serverName;
    private final FailureDetector failureDetector;
    private final BlackList<ServerPool> blackList;

    private ConcurrentLinkedQueue<Connection> pooledConnection = new ConcurrentLinkedQueue<>();
    private final int maxSize;
    private final AtomicInteger trackedSize = new AtomicInteger(0);
    private final DataSource connectionBuilder;
    private final int validationTimeout;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final PoolSizingStrategy poolSizingStrategy;

    ServerPool(DataSource connectionBuilder,
               String serverName,
               int maxPoolSize,
               FailureDetector failureDetector,
               PoolSizingStrategy poolSizingStrategy,
               BlackList<ServerPool> blackList,
               int validationTimeout){
        this.serverName=serverName;
        this.failureDetector=failureDetector;
        this.connectionBuilder = connectionBuilder;
        this.maxSize = maxPoolSize;
        this.poolSizingStrategy=poolSizingStrategy;
        this.validationTimeout = validationTimeout;
        this.blackList = blackList;
    }

    Connection tryAcquireConnection(String userName,String password) throws SQLException{
        while(true){
            Connection conn=pooledConnection.poll();
            if(conn!=null){
                if(conn.isClosed()) {
                    trackedSize.decrementAndGet();
                    continue;
                }
                if(validationTimeout<=0 || conn.isValid(validationTimeout)){
                    return wrapConnection(conn);
                } else{
                    conn.close(); //close this connection and try again
                }
            }else {
                boolean shouldContinue;
                do{
                    int currPoolSize = trackedSize.get();
                    if(currPoolSize>=maxSize) return null; //we have too many open already, so we cannot acquire a new one
                    shouldContinue = !trackedSize.compareAndSet(currPoolSize,currPoolSize+1);
                }while(shouldContinue);
                try{
                    return createConnection(userName,password);
                }catch(SQLNonTransientConnectionException se){
                    //this server is dead, blacklist it
                    logError("tryAcquire("+serverName+")",se);
                    blackList.blacklist(this);
                    trackedSize.decrementAndGet();
                    return null;
                }
            }
        }
    }


    Connection newConnection(String userName, String password) throws SQLException{
        trackedSize.incrementAndGet();
        return createConnection(userName,password);
    }


    boolean heartbeat() throws SQLException{
        try(Connection conn=acquireConnection()){
            try(Statement s=conn.createStatement()){
                s.execute("CALL SYSCS_UTIL.CHECK_SERVER_LIVENESS()");
                failureDetector.success();
                return true;
            }catch(SQLException se){
                return failureDetector.failed();
            }
        }
    }

    boolean isDead(){
        return !failureDetector.isAlive();
    }

    public void close() throws SQLException{
        if(!closed.compareAndSet(false,true)) return; //already been closed

        //close all pooled connections
        List<SQLException> errors = null;
        Connection toClose;
        int removed = 0;
        while((toClose = pooledConnection.poll())!=null){
            try{
                toClose.close();
                removed++;
            }catch(SQLException se){
                if(errors==null)
                    errors = new LinkedList<>();
                errors.add(se);
            }
        }
        if(trackedSize.get()>removed){
            throw new SQLException("Cannot close connection pool while there are outstanding connections",
                    SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION,1);
        }else if(errors!=null){
            if(errors.size()==1)
                throw errors.get(0);
            SQLException se = new SQLException("Unable to close connections", "SE001",1);
            for(SQLException underlying:errors){
               se.addSuppressed(underlying);
            }
            throw se;
        }
    }

    @Override
    public String toString(){
        return serverName;
    }

    /* ****************************************************************************************************************/
    /*private helper methods and classes*/

    private void logError(String operation,Throwable t){
        LOGGER.log(Level.SEVERE,"error during "+operation,t);
    }

    private Connection createConnection(String userName,String password) throws SQLException{
        if(userName!=null)
            return wrapConnection(connectionBuilder.getConnection(userName,password));
        else return wrapConnection(connectionBuilder.getConnection());
    }

    private Connection acquireConnection() throws SQLException{
        Connection conn = tryAcquireConnection(null,null);
        if(conn==null)
            conn = newConnection(null,null);
        return conn;
    }

    private Connection wrapConnection(Connection conn){
        return new PooledConnection(conn);
    }

    private class PooledConnection extends ErrorTrappingConnection{

        public PooledConnection(Connection delegate){
            super(delegate);
        }

        @Override
        protected void reportError(Throwable t){
            boolean close = false;
            if(t instanceof SQLNonTransientConnectionException){
                blackList.blacklist(ServerPool.this);
                close = true;
            } else if(t instanceof SQLTransientConnectionException){
                failureDetector.failed();
                close = true;
            }

            if(close){
                try{
                    delegate.close();
                }catch(SQLException e){
                    failureDetector.failed();
                }
            }
        }

        @Override
        protected Statement wrapStatement(Statement statement){
            return new ErrorTrappingStatement(statement){
                @Override
                protected void reportError(Throwable t){
                   PooledConnection.this.reportError(t);
                }
            };
        }

        @Override
        protected PreparedStatement wrapPrepare(PreparedStatement preparedStatement){
            //TODO -sf- implement
            return preparedStatement;
        }

        @Override
        protected CallableStatement wrapCall(CallableStatement callableStatement){
            //TODO -sf- implement
            return callableStatement;
        }

        @Override
        public void close() throws SQLException{
            int currTrackedSize = trackedSize.get();
            if(currTrackedSize>maxSize || delegate.isClosed()){
                    /*
                     * We've exceeded this pool size, so discard the underlying connection
                     * cleanly and then allow GC to occur.
                     */
                super.close();
                trackedSize.decrementAndGet(); //we are no longer tracking this connection
            }else{
                /*
                 * We are under the max pool size, so return this to the pool to ensure proper
                 * re-use
                 */
                pooledConnection.add(delegate);
                poolSizingStrategy.releasePermit();
            }
        }
    }
}
