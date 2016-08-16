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

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 8/15/16
 */
abstract class ErrorTrappingStatement implements Statement{
    protected Statement delegate;

    public ErrorTrappingStatement(){}

    ErrorTrappingStatement(Statement delegate){
        this.delegate=delegate;
    }

    protected Statement getDelegate() throws SQLException{
        return delegate;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException{
        try{
            return getDelegate().executeQuery(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException{
        try{
            return getDelegate().executeUpdate(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void close() throws SQLException{
        try{
            getDelegate().close();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException{
        try{
            return getDelegate().getMaxFieldSize();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException{
        try{
            getDelegate().setMaxFieldSize(max);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getMaxRows() throws SQLException{
        try{
            return getDelegate().getMaxRows();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException{
        try{
            getDelegate().setMaxRows(max);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException{
        try{
            getDelegate().setEscapeProcessing(enable);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException{
        try{
            return getDelegate().getQueryTimeout();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException{
        try{
            getDelegate().setQueryTimeout(seconds);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void cancel() throws SQLException{
        try{
            getDelegate().cancel();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        try{
            return getDelegate().getWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void clearWarnings() throws SQLException{
        try{
            getDelegate().clearWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException{
        try{
            getDelegate().setCursorName(name);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException{
        try{
            return getDelegate().execute(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException{
        try{
            return getDelegate().getResultSet();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException{
        try{
            return getDelegate().getUpdateCount();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException{
        try{
            return getDelegate().getMoreResults();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException{
        try{
            getDelegate().setFetchDirection(direction);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException{
        try{
            return getDelegate().getFetchDirection();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException{
        try{
            getDelegate().setFetchSize(rows);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getFetchSize() throws SQLException{
        try{
            return getDelegate().getFetchSize();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException{
        try{
            return getDelegate().getResultSetConcurrency();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetType() throws SQLException{
        try{
            return getDelegate().getResultSetType();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException{
        try{
            getDelegate().addBatch(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void clearBatch() throws SQLException{
        try{
            getDelegate().clearBatch();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException{
        try{
            return getDelegate().executeBatch();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Connection getConnection() throws SQLException{
        try{
            return getDelegate().getConnection();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException{
        try{
            return getDelegate().getMoreResults(current);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException{
        try{
            return getDelegate().getGeneratedKeys();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,int autoGeneratedKeys) throws SQLException{
        try{
            return getDelegate().executeUpdate(sql,autoGeneratedKeys);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,int[] columnIndexes) throws SQLException{
        try{
            return getDelegate().executeUpdate(sql,columnIndexes);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,String[] columnNames) throws SQLException{
        try{
            return getDelegate().executeUpdate(sql,columnNames);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,int autoGeneratedKeys) throws SQLException{
        try{
            return getDelegate().execute(sql,autoGeneratedKeys);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,int[] columnIndexes) throws SQLException{
        try{
            return getDelegate().execute(sql,columnIndexes);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,String[] columnNames) throws SQLException{
        try{
            return getDelegate().execute(sql,columnNames);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException{
        try{
            return getDelegate().getResultSetHoldability();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isClosed() throws SQLException{
        try{
            return getDelegate().isClosed();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException{
        try{
            getDelegate().setPoolable(poolable);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isPoolable() throws SQLException{
        try{
            return getDelegate().isPoolable();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException{
        try{
            getDelegate().closeOnCompletion();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException{
        try{
            return getDelegate().isCloseOnCompletion();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        return getDelegate().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return getDelegate().isWrapperFor(iface);
    }

    protected abstract void reportError(Throwable t);
}
