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

import com.splicemachine.test_dao.SchemaDAO;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static java.lang.String.format;


public class SpliceUserWatcher extends TestWatcher {
    private static final Logger LOG = Logger.getLogger(SpliceUserWatcher.class);
    public String userName;
    public String password;

    public SpliceUserWatcher(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    @Override
    protected void starting(Description description) {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            dropAndCreateUser(userName, password);
        } catch (Exception e) {
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
        LOG.trace("Finished");
    }

    public void createUser(String userName, String password) {

        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.prepareStatement("call syscs_util.syscs_create_user(?,?)");
            statement.setString(1, userName);
            statement.setString(2, password);
            statement.execute();
        } catch (Exception e) {
            LOG.error("error Creating " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public void dropUser(String userName) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.prepareStatement("select username from sys.sysusers where username = ?");
            statement.setString(1, userName.toUpperCase());
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                statement = connection.prepareStatement("call syscs_util.syscs_drop_user(?)");
                statement.setString(1, userName);
                statement.execute();
            }
        } catch (Exception e) {
            LOG.error("error Creating " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public void dropAndCreateUser(String userName, String password) {
        dropUser(userName);
        createUser(userName, password);
        dropSchema(userName);
    }

    public void dropSchema(String userName) {
        try (Connection connection = SpliceNetConnection.getConnection()) {
//            connection.setAutoCommit(false);

            SchemaDAO schemaDAO = new SchemaDAO(connection);
            try (ResultSet rs = connection.getMetaData().getSchemas(null, userName)) {
                if (rs.next()) {
                    schemaDAO.drop(rs.getString("TABLE_SCHEM"));
                }
            } catch (Exception e) {
                e.printStackTrace();
                connection.rollback();
                throw e;
            }
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
