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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Pool of Connections to a specific server.
 *
 * Error handling:
 *
 * There are a number of connection-related problems that can occur within this class (such as
 * SocketExceptions, etc). The contract of this class is that these are <em>rethrown without blacklisting</em>.
 * Specifically, it is the responsibility of the caller to deal with any connection-related exceptions
 * which may occur. However, these connection-related problems will be treated as a "failure", and the FailureDetector
 * will be notified.
 *
 * @author Scott Fines
 *         Date: 8/15/16
 */
class ServerPool implements Comparable<ServerPool>{
    /*
     * We use java.util.logging here to avoid requiring a logging jar dependency on our applications (and therefore
     * causing all kinds of potential dependency problems).
     */
    private static final Logger LOGGER = Logger.getLogger(ServerPool.class.getName());

    final String serverName;
    private final FailureDetector failureDetector;
    private final int maxSize;
    private final AtomicInteger trackedSize = new AtomicInteger(0);
    private final DataSource connectionBuilder;
    private final int validationTimeout;
    private final PoolSizingStrategy poolSizingStrategy;
    private ConcurrentLinkedQueue<Connection> pooledConnection = new ConcurrentLinkedQueue<Connection>(){
        @Override
        public Connection poll(){
            Connection conn=super.poll();
            if(LOGGER.isLoggable(Level.FINEST))
                LOGGER.finest("Polling connection "+ conn+" from pool");
            return conn;
        }
    };
    private AtomicBoolean closed = new AtomicBoolean(false);

    ServerPool(DataSource connectionBuilder,
               String serverName,
               int maxPoolSize,
               FailureDetector failureDetector,
               PoolSizingStrategy poolSizingStrategy,
               int validationTimeout){
        this.serverName=serverName;
        this.failureDetector=failureDetector;
        this.connectionBuilder = connectionBuilder;
        this.maxSize = maxPoolSize;
        this.poolSizingStrategy=poolSizingStrategy;
        this.validationTimeout = validationTimeout;
    }

    public void close() throws SQLException{
        if(!closed.compareAndSet(false,true)) return; //already been closed
        if(LOGGER.isLoggable(Level.FINE))
            LOGGER.fine("Closing pool for server "+ serverName);

        //close all pooled connections
        SQLException e = null;
        Connection toClose;
        int removed = 0;
        while((toClose = pooledConnection.poll())!=null){
            try{
                if(LOGGER.isLoggable(Level.FINEST))
                    LOGGER.finest("closing("+toClose+")");
                toClose.close();
                trackedSize.decrementAndGet();
                removed++;
            }catch(SQLException se){
                if(e==null) e =se;
                else{
                    e.setNextException(se);
                }
            }
        }

        if(e!=null){
            throw e;
        } else if(trackedSize.get()>removed){
            throw new SQLException("Cannot close connection pool while there are outstanding connections",
                    SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION,1);
        }
    }

//    @Override
//    public String toString(){
//        return serverName;
//    }

    @Override
    @SuppressWarnings("NullableProblems")
    public int compareTo(ServerPool o){
        return serverName.compareTo(o.serverName);
    }

    public boolean isClosed(){
        return closed.get();
    }

    int connectionCount(){
        return trackedSize.get();
    }

    @Override
    public String toString(){
        return "ServerPool{"+
                "serverName='"+serverName+'\''+
                ", closed="+closed.get()+
                ", pooledConnections="+pooledConnection+
                ", outstandingConnections="+trackedSize.get()+
                '}';
    }

    /* ****************************************************************************************************************/
    /*package-local methods*/
    Connection tryAcquireConnection(boolean validate) throws SQLException{
        boolean shouldContinue = true;
        do{
            Connection conn=pooledConnection.poll();
            if(conn!=null){
                if(LOGGER.isLoggable(Level.FINEST)){
                    LOGGER.finest("conn "+ conn+" may be available from pool");
                }
                if(conn.isClosed()) {
                    trackedSize.decrementAndGet();
                    continue;
                }
                if(!validate || validationTimeout<=0 || conn.isValid(validationTimeout)){
                    if(LOGGER.isLoggable(Level.FINEST))
                        LOGGER.finest("Acquiring "+conn+" from pool");
                    return wrapConnection(conn);
                } else{
                    if(LOGGER.isLoggable(Level.FINEST))
                        LOGGER.finest("Connection "+conn+" is not valid, closing");
                    trackedSize.decrementAndGet();
                    conn.close(); //close this connection and try again
                }
            }else {
                int currPoolSize = trackedSize.get();
                if(currPoolSize>=maxSize){
                    if(LOGGER.isLoggable(Level.FINER))
                        LOGGER.finer("Pool <"+serverName+"> is full");
                    return null; //we have too many open already, so we cannot acquire a new one
                }
                shouldContinue = !trackedSize.compareAndSet(currPoolSize,currPoolSize+1);
            }
        }while(shouldContinue);

        if(LOGGER.isLoggable(Level.FINEST)){
            LOGGER.finest("Opening new hard connection to "+serverName+"("+trackedSize.get()+","+pooledConnection.size()+")");
        }
        return createNewConnection("tryAcquire");
    }

    Connection newConnection() throws SQLException{
        if(LOGGER.isLoggable(Level.FINEST))
            LOGGER.finest("Creating new connection to server "+ serverName);
        trackedSize.incrementAndGet();
        return createNewConnection("newConnection");
    }


    boolean heartbeat(){
        if(isClosed()) return false; //don't continue validating, we are closed
        try(Connection conn=acquireConnection(false)){ //don't validate, since we are going to do that ourselves
            boolean oldAutoCommit = conn.getAutoCommit();
            if(!oldAutoCommit)
                conn.setAutoCommit(true);

            /*
             * since Connection.isValid() ensures that we actually talk to the server, we can
             * use it to determine if we can still talk to that server. Essentially, we say
             * "hey, if at least one connection is able to communicate with that server safely,
             * then we can't be dead yet".
             */
            try{
                if(conn.isValid(1)){
                    failureDetector.success();
                    if(LOGGER.isLoggable(Level.FINER))
                        LOGGER.finer("heartbeat for server "+serverName+" successful");
                    return true;
                }else{
                    if(LOGGER.isLoggable(Level.FINER))
                        LOGGER.finer("heartbeat for server "+serverName+" failed");
                    failureDetector.failed();
                    return false;
                }
            }finally{
                conn.setAutoCommit(oldAutoCommit);
            }
        }catch(SQLException se){
            logError("heartbeat",se,Level.INFO);
            failureDetector.failed();
            return false;
        }
    }

    boolean isDead(){
        return !failureDetector.isAlive();
    }


    /* ****************************************************************************************************************/
    /*private helper methods and classes*/
    private Connection createNewConnection(String op) throws SQLException{
        try{
            return createConnection();
        }catch(SQLException se){
            //throw the exception if appropriate
            logError(op+"("+this+")",se);
            /*
             * If it's a network error that we encountered, we should mark
             * it as such, then re-throw the error
             */
            if(ClientErrors.isNetworkError(se))
                failureDetector.failed();
            trackedSize.decrementAndGet();
            throw se;
        }
    }

    private void logError(String operation,Throwable t){
       logError(operation,t,Level.SEVERE);
    }
    private void logError(String operation,Throwable t,Level logLevel){
        String errorMessage= "error during "+operation+":";
        if(t instanceof SQLException){
            SQLException se = (SQLException)t;
            errorMessage +="["+se.getSQLState()+"]";
        }
        if(t.getMessage()!=null)
            errorMessage+=t.getMessage();

        LOGGER.log(logLevel,errorMessage,t);
    }

    private Connection createConnection() throws SQLException{
        return wrapConnection(connectionBuilder.getConnection());
    }

    private Connection acquireConnection(boolean validate) throws SQLException{
        Connection conn = tryAcquireConnection(validate);
        if(conn==null)
            conn = newConnection();
        return conn;
    }

    private Connection wrapConnection(Connection conn){
        return new PooledConnection(conn);
    }

    private static final AtomicInteger ID_GEN = new AtomicInteger(0);
    private class PooledConnection extends ErrorTrappingConnection{
        private final int connID = ID_GEN.incrementAndGet();
        public PooledConnection(Connection delegate){
            super(delegate);
        }

        @Override
        public String toString(){
            return "PooledConnection{"+serverName+"["+connID+"]}";
        }

        @Override
        public void close() throws SQLException{
            int currTrackedSize = trackedSize.get();
            if(LOGGER.isLoggable(Level.FINEST))
                LOGGER.finest("close("+this+")");
            if(ServerPool.this.isClosed() || isClosed() ||delegate.isClosed() || currTrackedSize>maxSize){
                if(LOGGER.isLoggable(Level.FINEST))
                    LOGGER.finest("closing connection to server "+serverName);
                /*
                 * We've exceeded this pool size, so discard the underlying connection
                 * cleanly and then allow GC to occur.
                 */
                super.close();
                trackedSize.decrementAndGet(); //we are no longer tracking this connection
            }else{
                if(LOGGER.isLoggable(Level.FINEST))
                    LOGGER.finest("releasing "+delegate+" to pool for server "+serverName);
                /*
                 * We are under the max pool size, so return this to the pool to ensure proper
                 * re-use
                 */
                pooledConnection.offer(delegate);
                poolSizingStrategy.releasePermit();
                if(LOGGER.isLoggable(Level.FINEST))
                    LOGGER.finest("pool size: "+ pooledConnection.size()+",tracked size: "+ trackedSize.get());
            }
        }

        @Override
        protected void reportError(Throwable t){
            if(t instanceof SQLException && ClientErrors.isNetworkError((SQLException)t))
                failureDetector.failed();
        }

        @Override
        protected Statement wrapStatement(Statement statement){
            return new ErrorTrappingStatement(statement){
                @Override protected void reportError(Throwable t){ PooledConnection.this.reportError(t); }
            };
        }

        @Override
        protected PreparedStatement wrapPrepare(PreparedStatement preparedStatement){
            return new ErrorTrappingPreparedStatement(preparedStatement){
                @Override protected void reportError(Throwable t){ PooledConnection.this.reportError(t); }
            };
        }

        @Override
        protected CallableStatement wrapCall(CallableStatement callableStatement){
            return new ErrorTrappingCallableStatement(callableStatement){
                @Override protected void reportError(Throwable t){ PooledConnection.this.reportError(t); }
            };
        }
    }
}
