/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public abstract class AbstractSpliceFunctionWatcher extends TestWatcher {
    private static final Logger LOG = Logger.getLogger(AbstractSpliceFunctionWatcher.class);
    protected String functionName;
    protected String schemaName;
    protected String createString;
    protected String userName;
    protected String password;
    public AbstractSpliceFunctionWatcher(String functionName,String schemaName, String createString) {
        this.functionName = functionName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.createString = createString;
    }
    public AbstractSpliceFunctionWatcher(String functionName,String schemaName, String createString, String userName, String password) {
        this(functionName, schemaName, createString);
        this.userName = userName;
        this.password = password;
    }

    @Override
    protected void starting(Description description) {
        LOG.trace("Starting");
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            SpliceNetConnection.ConnectionBuilder connectionBuilder = SpliceNetConnection.newBuilder();
            if (userName != null) {
                connectionBuilder.user(userName).password(password);
            }
            connection = connectionBuilder.build();
            dropIfExists(connection);
            statement = connection.createStatement();
            statement.execute( String.format("CREATE %s %s.%s %s", functionType(), schemaName, functionName, createString));
            connection.commit();
        } catch (Exception e) {
            LOG.error(String.format("Create %s statement is invalid ", functionType()));
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
        super.starting(description);
    }
    @Override
    protected void finished(Description description) {
        LOG.trace("finished");
        executeDrop(schemaName,functionName);
    }

    protected void executeDrop(String schemaName,String functionName) {
        LOG.trace("executeDrop");
        Connection connection = null;
        Statement statement = null;
        try {
            connection = SpliceNetConnection.getDefaultConnection();
            statement = connection.createStatement();
            statement.execute(String.format("DROP %s %s.%s", functionType(), schemaName.toUpperCase(), functionName.toUpperCase()));
            connection.commit();
        } catch (Exception e) {
            LOG.error("error Dropping " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    protected abstract String functionType();

    protected abstract void dropIfExists(Connection c) throws SQLException;
}

