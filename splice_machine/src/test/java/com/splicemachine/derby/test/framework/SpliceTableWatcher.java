/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.test.framework;

import com.splicemachine.test_dao.TableDAO;
import org.apache.commons.dbutils.DbUtils;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import java.sql.*;

public class SpliceTableWatcher extends TestWatcher {
    public String tableName;
    protected String schemaName;
    protected String createString;
    protected String userName;
    protected String password;
    protected Connection connection;
    private boolean shouldClose = true;


    public SpliceTableWatcher(String tableName,String schemaName, String createString) {
        this.tableName = tableName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.createString = createString;
    }

    public SpliceTableWatcher(String tableName,String schemaName, String createString, Connection connection) {
        this(tableName,schemaName,createString);
        this.connection = connection;
    }

    public SpliceTableWatcher(String tableName,String schemaName, String createString, Connection connection, boolean shouldClose) {
        this(tableName,schemaName,createString);
        this.connection = connection;
        this.shouldClose = shouldClose;
    }

    public SpliceTableWatcher(String tableName, String schemaName, String createString, String userName, String password) {
        this(tableName,schemaName,createString);
        this.userName = userName;
        this.password = password;
    }

    @Override
    protected void starting(Description description) {
        start();
        super.starting(description);
    }

    @Override
    protected void finished(Description description) {
        if (shouldClose)
            DbUtils.commitAndCloseQuietly(connection);
    }

    public void importData(String filename) {
        PreparedStatement ps = null;
        try {
            ps = getConnection().prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,null,null,null,1,null,true,'utf-8')");
            ps.setString(1,schemaName);
            ps.setString(2,tableName);
            ps.setString(3,filename);
            try(ResultSet rs = ps.executeQuery()){
                while(rs.next()){

                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(ps);
        }
    }

    public void importData(String filename, String timestamp) {
        PreparedStatement ps = null;
        try {
            ps = getConnection().prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,?,null,null,0,null,true,null)");
            ps.setString(1,schemaName);
            ps.setString(2,tableName);
            ps.setString(3,filename);
            ps.setString(4, timestamp);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(ps);
        }
    }

    @Override
    public String toString() {
        return schemaName+"."+tableName;
    }

    public String getSchema() {
        return schemaName;
    }

    //-----------------------------------------------------------------------------------------
    // The following methods are for tagging the log messages with additional information
    // related to the schema and table.
    //-----------------------------------------------------------------------------------------

    public void start() {
        try {
            createTable();
            return; // bail
        } catch (Exception e) {
            // Swallow, issue
        }

        Statement statement = null;
        ResultSet rs;
        synchronized(SpliceTableWatcher.class){
            try{
                rs=getConnection().getMetaData().getTables(null,schemaName,tableName,null);
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }
        TableDAO tableDAO = new TableDAO(connection);

        try {
            if (rs.next()) {
                tableDAO.drop(schemaName, tableName);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(rs);
        }
        try {
            createTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createTable() throws Exception {
        Statement statement = null;
        try{
            statement = getConnection().createStatement();
            statement.execute(String.format("create table %s.%s %s",schemaName,tableName,createString));
            connection.commit();
        } finally {
            DbUtils.closeQuietly(statement);
        }
    }

    private Connection getConnection() throws SQLException {
        if (connection == null)
            connection=(userName==null)?SpliceNetConnection.getConnection():SpliceNetConnection.getConnectionAs(userName,password);
        return connection;
    }

}
