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
 */

package com.splicemachine.derby.transactions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test procedures to install in the splice server.
 * <p/>
 * For an example as to how to install and run one of these procedures, see QueryTimeoutIT
 *
 * @author Jeff Cunningham
 *         Date: 12/8/14
 */
public class TestProcs {
    public TestProcs() {}

    /**
     * A test procedure created to cause a manufactured delay in a client's update by sleeping a configurable
     * amount of time <i>after</i> the update is executed.
     * This procedure was created for QueryTimeoutIT to assure
     * a query timeout occurs.
     *
     * @param updateString SQL string that will be used to update a table.
     * @param sleepSecs the time the procedure should sleep before returning.
     * @param rs the returned result set.
     * @throws SQLException if the update fails.
     */
    public static void UPDATE_WITH_TIMEOUT_AFTER(String updateString, int sleepSecs, ResultSet[] rs) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            statement = conn.createStatement();
            statement.executeUpdate(updateString);
            rs[0] = statement.getResultSet();
            // sleep
            try {
                Thread.sleep(sleepSecs*1000);
            } catch (InterruptedException e) {
                throw new SQLException("Sleep failed: ", e);
            }
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }
    }

    /**
     * A test procedure created to cause a manufactured delay in a client's update by sleeping a configurable
     * amount of time <i>before</i> the update is executed.
     * This procedure was created for QueryTimeoutIT to assure
     * a query timeout occurs.
     *
     * @param updateString SQL string that will be used to update a table.
     * @param sleepSecs the time the procedure should sleep before returning.
     * @param rs the returned result set.
     * @throws SQLException if the update fails.
     */
    public static void UPDATE_WITH_TIMEOUT_BEFORE(String updateString, int sleepSecs, ResultSet[] rs) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            statement = conn.createStatement();
            // sleep
            try {
                Thread.sleep(sleepSecs*1000);
            } catch (InterruptedException e) {
                throw new SQLException("Sleep failed: ", e);
            }
            statement.executeUpdate(updateString);
            rs[0] = statement.getResultSet();
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
