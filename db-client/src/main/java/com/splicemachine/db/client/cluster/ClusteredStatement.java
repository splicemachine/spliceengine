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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
class ClusteredStatement implements Statement,Debuggable{
    private static final Logger LOGGER=Logger.getLogger(ClusteredStatement.class.getName());
    private final ClusteredConnection sourceConnection;
    final ClusteredConnManager connManager;
    //immutable statement configuration
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    //immutable networking properties (i.e. retries, etc.)
    final int executionRetries;

    /*
     * Using transient to indicate that these may change. In fact,
     * they may change on a query-by-query basis, depending on how
     * the error handling is dealt with. Generally speaking, though,
     * these will remain constant as long as a connection to a database node
     * continues to be successful (i.e. no errors)
     *
     * At some point in the future, we may consider load-balancing
     * the actual requests when autocommit mode is on, but for now, performance
     * dictates that we keep the connection and statement objects unless there is a
     * reason to discard them
     */
    private transient RefCountedConnection currentConn;
    private transient Statement currStatement;

    private boolean closed = false;

    /*
     * Mutable properties for the statement.
     *
     * The properties are initialized to their JDBC-required default values.
     */
    private boolean closeOnCompletion = false;
    private boolean isPoolable = false;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    private int fetchSize = 0;
    private int maxFieldSize = 0;
    private int maxRows = 0;
    private boolean escapeProcessing = true;
    private int queryTimeout = 0;


    private List<String> sqlBatch = null;

    ClusteredStatement(ClusteredConnection sourceConnection,
                       ClusteredConnManager connManager,
                       int resultSetType,
                       int resultSetConcurrency,
                       int resultSetHoldability,
                       int executionRetries){
        this.sourceConnection=sourceConnection;
        this.connManager=connManager;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.executionRetries = executionRetries;
    }

    @Override
    public Connection getConnection() throws SQLException{
        return sourceConnection;
    }

    @Override
    public void close() throws SQLException{
        if(closed) return;
        closed = true;
        SQLException se = null;
        try{
            if(currStatement!=null)
                currStatement.close();
        }catch(SQLException e){
            se = e;
        }

        try{
            if(currentConn!=null)
                currentConn.release();
        }catch(SQLException e){
            if(se==null) se = e;
            else se.setNextException(e);
        }

        connManager.releaseStatement(this);
        if(se!=null)
            throw se;
    }

    @Override
    public boolean isClosed() throws SQLException{
        return closed;
    }

    @Override
    public void cancel() throws SQLException{
        throw new SQLFeatureNotSupportedException("cancel");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException{
        checkClosed();
        reopenIfNecessary();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            try{
                Statement currStatement=getDelegateStatement();
                ResultSet rs =currStatement.executeQuery(sql);
                if(collector!=null){
                    SQLWarning sw=currStatement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                /*
                 *  Unlike updates, we can't retry queries once a row of the query has been
                 *  returned (or we risk double-returning rows). This means we can't do much
                 *  retrying in the RETURNED ResultSet, but it doesn't mean that we can't re-issue
                 *  the query if it fails before a ResultSet has returned, which is what we do here.
                 */
                return rs;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }


    @Override
    public int executeUpdate(String sql) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                int count=statement.executeUpdate(sql);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return count;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }


    @Override
    public int executeUpdate(String sql,int autoGeneratedKeys) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                int count=statement.executeUpdate(sql,autoGeneratedKeys);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return count;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public int executeUpdate(String sql,int[] columnIndexes) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                int count=statement.executeUpdate(sql,columnIndexes);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return count;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public int executeUpdate(String sql,String[] columnNames) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                int count=statement.executeUpdate(sql,columnNames);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return count;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public boolean execute(String sql,int autoGeneratedKeys) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                boolean result=statement.execute(sql,autoGeneratedKeys);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return result;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public boolean execute(String sql,int[] columnIndexes) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                boolean result=statement.execute(sql,columnIndexes);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return result;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public boolean execute(String sql,String[] columnNames) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                boolean result=statement.execute(sql,columnNames);
                if(collector!=null){
                    SQLWarning sw=statement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return result;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                    reopenIfNecessary();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public boolean execute(String sql) throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            reopenIfNecessary();
            try{
                Statement statement = getDelegateStatement();
                boolean result=statement.execute(sql);
                if(collector!=null){
                    SQLWarning sw=currStatement.getWarnings();
                    if(sw!=null)
                        sw.setNextException(collector);
                }
                return result;
            }catch(SQLException se){
                if(collector==null) collector = se;
                else collector.setNextException(se);
                if(!ClientErrors.isNetworkError(se)){
                    throw collector;
                }else if(connManager.isAutoCommit()){
                    disconnect();
                }else throw collector; //if we aren't in auto-commit mode, then we can't retry
            }
            numTries--;
        }
        throw collector;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        if(s==null) return null;
        return s.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        if(s!=null){
            s.clearWarnings();
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException{
        /*
         * For now, we don't support this because it requires some global
         * checking to ensure that cursor names aren't set the same way. Derby
         * resolved this by performing the duplicate cursor check at execution time, but
         * that's actually not spec-compliant, so we'll have to evaluate how to perform
         * the same logic in a more spec-compliant way at some point. Since this doesn't
         * really make any sense to use until positioned updates/deletes are available to us,
         * it's not too big of a deal to not support this just yet.
         */
        throw new SQLFeatureNotSupportedException("setCursorName");
    }

    @Override
    public ResultSet getResultSet() throws SQLException{
        checkClosed();
        /*
         * Note that we do no retries here--we are only able of performing retries during
         * actual statement execution.
         */
        Statement s = getDelegateStatement();
        if(s==null) return null;
        return s.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        if(s==null) return -1;
        return s.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        return s!=null && s.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        return s!=null && s.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException{
        checkClosed();
        Statement s = getDelegateStatement();
        if(s==null) return null;
        return s.getGeneratedKeys();
    }

    /* ***************************************************************************************************/
    /*batch execution methods*/
    @Override
    public void addBatch(String sql) throws SQLException{
        checkClosed();
        /*
         * We keep track of the batch of executions ourselves, rather than passing on
         * to the delegate for 2 reasons:
         *
         * 1. we may not HAVE a delegate (i.e. we may not have constructed an underlying
         * connection yet)
         * 2. the statement might fail, and if auto-commit is enabled, then we'll need to
         * be able to retry the batch in some meaningful way (if possible given our "non-atomic
         * batch failure" situation). This means we'll need to carry a batch over from
         * one statement to another.
         */
        if(sqlBatch==null){
            /*
             * A common execution pattern is to take a large number of statements,
             * break them up into fixed size batches, and then run them one batch at a time.
             * In this pattern the batch size remains fixed throughout the operation, which
             * means that it's actually less wasteful of memory for us to use a fixed-allocation
             * array rather than a linked list. Of course, the first run is more wasteful because
             * we have to allow the array list time to expand, but c'est la vie. We will
             * use the trimToSize() method to eliminate extra space after the first run.
             */
            sqlBatch=new ArrayList<>();
        }
        sqlBatch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException{
        checkClosed();
        sqlBatch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException{
        checkClosed();
        reopenIfNecessary();
        if(sqlBatch==null || sqlBatch.size()<=0) return new int[]{}; //don't execute empty SQL

        /*
         * we trim to size here to improve memory usage when we are using the common
         * pattern of fixed batch sizes. In 99.9999999% of all cases, sqlBatch will be an
         * ArrayList. However, we put the if() check in to protect against future implementation
         * changes by unsuspecting developers (although we put a note with the instantiation as
         * well...). Since it will always be true, the JVM is likely to strip the if() statement
         * out during compilation anyway.
         */
        if(sqlBatch instanceof ArrayList)((ArrayList)sqlBatch).trimToSize();

        Statement s = getDelegateStatement();
        for(String sql:sqlBatch){
            s.addBatch(sql);
        }
        int numTries = executionRetries;
        SQLException collector = null;
        while(numTries>0){
            try{
                return s.executeBatch();
            }catch(SQLException se){
                if(collector==null)
                    collector = se;
                else collector.setNextException(se);

                if(!ClientErrors.isNetworkError(se))
                    throw collector;
            }
            numTries--;
        }
        //TODO -sf- throw a different SQL exception?
        throw collector;
    }

    /* ****************************************************************************************************************/
    /* property methods*/
    @Override
    public int getResultSetConcurrency() throws SQLException{
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException{
        return resultSetType;
    }

    @Override
    public int getResultSetHoldability() throws SQLException{
        return resultSetHoldability;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException{
        checkClosed();
        switch(direction){
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                this.fetchDirection = direction;
                Statement s = getDelegateStatement();
                if(s!=null)
                    s.setFetchDirection(fetchDirection);
                return;
            default:
                throw new SQLException(null,SQLState.INVALID_FETCH_DIRECTION);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException{
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException{
        checkClosed();
        if(rows<0)
            throw new SQLException(null,SQLState.INVALID_ST_FETCH_SIZE);
        this.fetchSize = rows;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException{
        return fetchSize;
    }

    @Override
    public int getMaxFieldSize() throws SQLException{
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException{
        checkClosed();
        if(max<0)
            throw new SQLException(null,SQLState.INVALID_MAXFIELD_SIZE);
        this.maxFieldSize = max;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setFetchSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException{
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException{
        checkClosed();
        if(max<0)
            throw new SQLException(null,SQLState.INVALID_MAX_ROWS_VALUE);
        this.maxRows = max;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setMaxRows(maxRows);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException{
        checkClosed();
        this.escapeProcessing = enable;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setEscapeProcessing(escapeProcessing);
    }

    @Override
    public int getQueryTimeout() throws SQLException{
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException{
        checkClosed();
        if(seconds <0)
            throw new SQLException(null,SQLState.INVALID_QUERYTIMEOUT_VALUE);
        this.queryTimeout = seconds;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setQueryTimeout(seconds);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException{
        checkClosed();
        this.isPoolable = poolable;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException{
        return isPoolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException{
        closeOnCompletion = true;
        Statement s = getDelegateStatement();
        if(s!=null)
            s.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException{
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLFeatureNotSupportedException("unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    public void logDebugInfo(Level level){
        if(!LOGGER.isLoggable(level)) return;
        String debugLine = "ClusteredStatement: {\n"
                +"currentConn: "+currentConn+",\n"
                +"currentStatement: "+currStatement+"}"
                ;
        LOGGER.log(level,debugLine);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void forceReaquire(){
        try{
            currentConn = connManager.forceReacquireConnection();
        }catch(SQLException e){
            ClusterUtil.logError(LOGGER,"reaquireConnection",e);
        }
    }

    protected void disconnect(){
        if(currStatement!=null){
            try{
                currStatement.close();
            }catch(SQLException ignored){}
            currStatement = null;
        }
        if(currentConn!=null){
            try{
                currentConn.release();
            }catch(SQLException ignored){}
            currentConn = null;
        }
        forceReaquire();
    }

    void checkClosed() throws SQLException{
        if(closed)
            throw new SQLException("Statement closed",SQLState.LANG_STATEMENT_CLOSED_NO_REASON);
    }

    final void reopenIfNecessary() throws SQLException{
        if(currStatement!=null) return;

        currentConn = connManager.acquireConnection();
        currentConn.acquire();
        currStatement = newStatement(currentConn.element());
        setProperties(currStatement);
    }

    protected Statement newStatement(Connection delegateConnection) throws SQLException{
        return delegateConnection.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
    }

    protected void setProperties(Statement s) throws SQLException{
        //set the properties for this connection
        s.setPoolable(isPoolable);
        s.setFetchSize(fetchSize);
        s.setFetchDirection(fetchDirection);
        s.setMaxFieldSize(maxFieldSize);
        s.setMaxRows(maxRows);
        s.setQueryTimeout(queryTimeout);
        s.setEscapeProcessing(escapeProcessing);
    }

    private Statement getDelegateStatement() throws SQLException{
        Statement currStatement=this.currStatement;
        currStatement.clearWarnings();
        currStatement.clearBatch();
        return currStatement;
    }

}
