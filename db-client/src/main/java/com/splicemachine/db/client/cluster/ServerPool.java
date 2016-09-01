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
import java.net.ConnectException;
import java.net.UnknownHostException;
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
    private static final int DEFAULT_MAX_CONNECT_ATTEMPTS=3;

    final String serverName;
    private final FailureDetector failureDetector;
    private final BlackList<ServerPool> blackList;
    private final int maxSize;
    private final AtomicInteger trackedSize = new AtomicInteger(0);
    private final DataSource connectionBuilder;
    private final int validationTimeout;
    private final PoolSizingStrategy poolSizingStrategy;
    private final int maxConnectAttempts;
    private ConcurrentLinkedQueue<Connection> pooledConnection = new ConcurrentLinkedQueue<>();
    private AtomicBoolean closed = new AtomicBoolean(false);

    ServerPool(DataSource connectionBuilder,
               String serverName,
               int maxPoolSize,
               FailureDetector failureDetector,
               PoolSizingStrategy poolSizingStrategy,
               BlackList<ServerPool> blackList,
               int validationTimeout){
       this(connectionBuilder,
               serverName,
               maxPoolSize,
               failureDetector,
               poolSizingStrategy,
               blackList,
               validationTimeout,
               DEFAULT_MAX_CONNECT_ATTEMPTS);
    }

    ServerPool(DataSource connectionBuilder,
               String serverName,
               int maxPoolSize,
               FailureDetector failureDetector,
               PoolSizingStrategy poolSizingStrategy,
               BlackList<ServerPool> blackList,
               int validationTimeout,
               int maxConnectAttempts){
        this.serverName=serverName;
        this.failureDetector=failureDetector;
        this.connectionBuilder = connectionBuilder;
        this.maxSize = maxPoolSize;
        this.poolSizingStrategy=poolSizingStrategy;
        this.validationTimeout = validationTimeout;
        this.blackList = blackList;
        this.maxConnectAttempts = maxConnectAttempts;
    }

    public void close() throws SQLException{
        if(!closed.compareAndSet(false,true)) return; //already been closed

        //close all pooled connections
        SQLException e = null;
        Connection toClose;
        int removed = 0;
        while((toClose = pooledConnection.poll())!=null){
            try{
                toClose.close();
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

    @Override
    public String toString(){
        return serverName;
    }

    /* ****************************************************************************************************************/
    /*package-local methods*/
    Connection tryAcquireConnection(boolean validate) throws SQLException{
        while(true){
            Connection conn=pooledConnection.poll();
            if(conn!=null){
                if(conn.isClosed()) {
                    trackedSize.decrementAndGet();
                    continue;
                }
                if(!validate || validationTimeout<=0 || conn.isValid(validationTimeout)){
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
                return createNewConnection("tryAcquire");
            }
        }
    }

    Connection newConnection() throws SQLException{
        trackedSize.incrementAndGet();
        return createNewConnection("newConnection");
    }


    boolean heartbeat() throws SQLException{
        try(Connection conn=acquireConnection(false)){ //don't validate, since we are going to do that ourselves
            /*
             * since Connection.isValid() ensures that we actually talk to the server, we can
             * use it to determine if we can still talk to that server. Essentially, we say
             * "hey, if at least one connection is able to communicate with that server safely,
             * then we can't be dead yet".
             */
            if(conn.isValid(1)){
                failureDetector.success();
                return true;
            }else{
                failureDetector.failed();
                return false;
            }
        }catch(SQLException se){
            logError("heartbeat",se,Level.INFO);
            return failureDetector.failed();
        }
    }

    boolean isDead(){
        return !failureDetector.isAlive();
    }

    /* ****************************************************************************************************************/
    /*private helper methods and classes*/
    private Connection createNewConnection(String op) throws SQLException{
        int attemptsRemaining = maxConnectAttempts;
        SQLException e = null;
        while(attemptsRemaining>0){
            attemptsRemaining--;
            try{
                return createConnection();
            }catch(SQLNonTransientConnectionException se){
                //throw the exception if appropriate
                dealWithNonTransientErrors(op,se);
                if(e==null) e =se;
                else {
                    e.setNextException(se);
                }
            }
        }

        trackedSize.decrementAndGet();
        blackList.blacklist(this);
        //e is not null because we would have returned before otherwise
        throw e;
    }

    private void dealWithNonTransientErrors(String op,SQLNonTransientConnectionException se) throws SQLNonTransientConnectionException{
        /*
         * Look at the error, and determine whether or not it should be thrown, or whether
         * a retry is allowed. If a retry is allowed, then return true. Otherwise, throw the exception
         */
        logError(op+"("+this+")",se);
        switch(se.getSQLState()){
            //the following are errors that allow connection retries
            case SQLState.DRDA_CONNECTION_TERMINATED:
            case SQLState.NO_CURRENT_CONNECTION:
            case SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED:
            case SQLState.CONNECT_SOCKET_EXCEPTION:
            case SQLState.SOCKET_EXCEPTION:
            case SQLState.CONNECT_UNABLE_TO_CONNECT_TO_SERVER:
            case SQLState.CONNECT_UNABLE_TO_OPEN_SOCKET_STREAM:
                return;
            case "08001":
                /*
                 * This is a generic connection error, that derby exceptionfactory didn't parse; this
                 * means that we are wrapping an underlying exception, so we need to parse that
                 */
                @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = ErrorUtils.getRootCause(se);
                //put specific return exceptions here
                if(t instanceof ConnectException
                        || t instanceof UnknownHostException) return;
            default:
                /*
                 * This exception is categorized as Connection severity or higher, and
                 * cannot be retried (we are getting errors that the server is telling us will
                 * never be correct, like bad authorization or database names). Therefore,
                 * we should blacklist ourselves and then throw the exception directly
                 */
                trackedSize.decrementAndGet();
                blackList.blacklist(this);
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

    private class PooledConnection extends ErrorTrappingConnection{

        public PooledConnection(Connection delegate){
            super(delegate);
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

        @Override
        protected void reportError(Throwable t){
            if(t instanceof SQLNonTransientConnectionException ||
                    t instanceof SQLTransientConnectionException){
                failureDetector.failed();
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
    }
}
