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

package com.splicemachine.triggers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Stored Procedures for trigger testing.
 * Test procedures to install in the splice server.
 * <p/>
 * For an example as to how to install and run these procedures, see Trigger_Exec_Stored_Proc_IT in
 * splice_machine_test.
 *
 *
 */
public class TriggerProcs {
    private static final String url="jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;clustered=false";

    /**
     * A stored procedure which gets called from a trigger.<br/>
     * Updates an "audit" table with the name of the user who initiated the trigger
     * and the timestamp of execution.
     * @param schema the name of the schema in which the table to update lives
     * @param table the name of the "audit" table
     * @throws SQLException
     */
    public static void proc_call_audit(String schema, String table) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(url);
            String username = conn.getMetaData().getUserName();
            statement = conn.createStatement();
            statement.execute(String.format("insert into %s.%s (username,insert_time) values ('%s', CURRENT_TIMESTAMP)",
                                            schema,table, username));
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }

    }

    /**
     * A stored procedure which gets called from a ROW level trigger.<br/>
     * Updates an "audit" table with the name of the user who initiated the trigger,
     * the timestamp of execution and, if row trigger, the new transition value and the old transition value.
     * @param schema the name of the schema in which the table to update lives
     * @param table the name of the "audit" table
     * @param newColValue the value of the new transition variable
     * @param oldColValue the value of the old transition variable
     * @throws SQLException
     */
    public static void proc_call_audit_with_transition(String schema, String table,
                                                       Integer newColValue, Integer oldColValue) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(url);
            String username = conn.getMetaData().getUserName();
            statement = conn.createStatement();
            statement.execute(
                String.format("insert into %s.%s (username,insert_time,new_id,old_id) values ('%s',CURRENT_TIMESTAMP, %s, %s)",
                                            schema,table, username, newColValue, oldColValue));
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }

    }

    /**
     * A stored procedure which gets called from a trigger.<br/>
     * Updates an "audit" table with the name of the user who initiated the trigger
     * and the timestamp of execution.
     * @param schema the name of the schema in which the table to update lives
     * @param table the name of the "audit" table
     * @throws SQLException
     */
    public static void proc_call_audit_with_result(String schema, String table, ResultSet[] rs) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(url);
            String username = conn.getMetaData().getUserName();
            statement = conn.createStatement();
            statement.execute(String.format("insert into %s.%s (username,insert_time) values ('%s', CURRENT_TIMESTAMP)",
                                            schema,table, username));
            rs[0] = statement.getResultSet();
        } finally {
            closeQuietly(statement);
            closeQuietly(conn);
        }

    }

    /**
     * A stored procedure which gets called from a trigger.<br/>
     * Executes the given sql.
     * @param sqlText the SQL to execute
     * @throws SQLException
     */
    public static void proc_exec_sql(String sqlText, ResultSet[] rs) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = DriverManager.getConnection(url);
            statement = conn.createStatement();
            statement.execute(sqlText);
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
