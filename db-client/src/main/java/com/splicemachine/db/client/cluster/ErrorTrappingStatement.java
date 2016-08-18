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

    ErrorTrappingStatement(Statement delegate){
        this.delegate=delegate;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException{
        try{
            return delegate.executeQuery(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException{
        try{
            return delegate.executeUpdate(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void close() throws SQLException{
        try{
            delegate.close();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException{
        try{
            return delegate.getMaxFieldSize();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException{
        try{
            delegate.setMaxFieldSize(max);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getMaxRows() throws SQLException{
        try{
            return delegate.getMaxRows();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException{
        try{
            delegate.setMaxRows(max);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException{
        try{
            delegate.setEscapeProcessing(enable);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException{
        try{
            return delegate.getQueryTimeout();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException{
        try{
            delegate.setQueryTimeout(seconds);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void cancel() throws SQLException{
        try{
            delegate.cancel();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        try{
            return delegate.getWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void clearWarnings() throws SQLException{
        try{
            delegate.clearWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException{
        try{
            delegate.setCursorName(name);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException{
        try{
            return delegate.execute(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException{
        try{
            return delegate.getResultSet();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException{
        try{
            return delegate.getUpdateCount();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException{
        try{
            return delegate.getMoreResults();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException{
        try{
            delegate.setFetchDirection(direction);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getFetchDirection() throws SQLException{
        try{
            return delegate.getFetchDirection();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException{
        try{
            delegate.setFetchSize(rows);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getFetchSize() throws SQLException{
        try{
            return delegate.getFetchSize();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException{
        try{
            return delegate.getResultSetConcurrency();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetType() throws SQLException{
        try{
            return delegate.getResultSetType();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException{
        try{
            delegate.addBatch(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void clearBatch() throws SQLException{
        try{
            delegate.clearBatch();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException{
        try{
            return delegate.executeBatch();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Connection getConnection() throws SQLException{
        try{
            return delegate.getConnection();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException{
        try{
            return delegate.getMoreResults(current);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException{
        try{
            return delegate.getGeneratedKeys();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,int autoGeneratedKeys) throws SQLException{
        try{
            return delegate.executeUpdate(sql,autoGeneratedKeys);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,int[] columnIndexes) throws SQLException{
        try{
            return delegate.executeUpdate(sql,columnIndexes);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql,String[] columnNames) throws SQLException{
        try{
            return delegate.executeUpdate(sql,columnNames);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,int autoGeneratedKeys) throws SQLException{
        try{
            return delegate.execute(sql,autoGeneratedKeys);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,int[] columnIndexes) throws SQLException{
        try{
            return delegate.execute(sql,columnIndexes);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean execute(String sql,String[] columnNames) throws SQLException{
        try{
            return delegate.execute(sql,columnNames);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException{
        try{
            return delegate.getResultSetHoldability();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isClosed() throws SQLException{
        try{
            return delegate.isClosed();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException{
        try{
            delegate.setPoolable(poolable);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isPoolable() throws SQLException{
        try{
            return delegate.isPoolable();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException{
        try{
            delegate.closeOnCompletion();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException{
        try{
            return delegate.isCloseOnCompletion();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return delegate.isWrapperFor(iface);
    }

    protected abstract void reportError(Throwable t);
}
