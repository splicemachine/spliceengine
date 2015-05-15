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
 * <b>NOTE:</b> Any change to this file should be manually compiled into splice_machine_test/src/test/resources/trigger_procs.jar
 * in order for the new procedures to be available.<br/>
 * The manual steps are:
 * <ol>
 *     <li>
 *         Compile the project
 *     </li>
 *     <li>
 *         <code>cd splice_machine_test/target/test-classes</code>
 *     </li>
 *     <li>
 *         <code>jar cvf trigger_procs.jar com/splicemachine/triggers/TriggerProcs.class</code>
 *     </li>
 *     <li>
 *         <code>cd ../..</code> (you're at splice_machine_test)
 *     </li>
 *     <li>
 *         <code>mv target/test-classes/trigger_procs.jar src/test/resources</code>
 *     </li>
 *     <li>
 *         Test and commit the new jar. <b>Note:</b> the jar changes don't show up with git status because jars are
 *         ignored in the top-level .gitignore. You need to use <code>git add -f ...</code> to get changes committed.
 *     </li>
 * </ol>
 * For an example as to how to install and run these procedures, see
 * {@link Trigger_Exec_Stored_Proc_IT}
 *
 *
 */
public class TriggerProcs {

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
            conn = DriverManager.getConnection("jdbc:default:connection");
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
            conn = DriverManager.getConnection("jdbc:default:connection");
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
