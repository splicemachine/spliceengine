/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceRoleWatcher extends TestWatcher {
    private static final Logger LOG = Logger.getLogger(SpliceRoleWatcher.class);
    protected String roleName;
    public SpliceRoleWatcher(String roleName) {
        this.roleName = roleName;
    }


    @Override
    protected void starting(Description description) {
        LOG.trace("Starting");
        executeDrop(roleName.toUpperCase());
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            connection = SpliceNetConnection.getDefaultConnection();
            statement = connection.createStatement();
            statement.execute(String.format("create role %s",roleName));
            connection.commit();
        } catch (Exception e) {
            LOG.error("Role statement is invalid ");
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
    }

    public static void executeDrop(String roleName) {
        LOG.trace("ExecuteDrop");
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = SpliceNetConnection.getDefaultConnection();
            statement = connection.prepareStatement("select roleid from sys.sysroles where roleid = ?");
            statement.setString(1, roleName);
            ResultSet rs = statement.executeQuery();
            if (rs.next())
                connection.createStatement().execute(String.format("drop role %s",roleName));
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

}
