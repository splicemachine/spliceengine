/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceTestDataSource;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimetravelIT {

    private static final String CLASS_NAME = TimetravelIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A",schemaWatcher.schemaName,
            "(a1 int)");
    protected static final SpliceTableWatcher B_TABLE = new SpliceTableWatcher("B",schemaWatcher.schemaName,
            "(i int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(B_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.execute("insert into a values 1,2");
                        try (PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into a select a1 + (select count(*) from a) from a")) {
                            for (int i = 0; i < 10; i++) {
                                ps.execute();
                            }
                        }
                        spliceClassWatcher.execute("insert into b values 0,1,2,3");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private TestConnection conn;

    @Before
    public void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }


    private static final String DB_URL_TEMPLATE = SpliceTestDataSource.DB_URL_TEMPLATE + ";schema=" + CLASS_NAME;

    private String getDefaultURL() {
        return String.format(DB_URL_TEMPLATE,
                SpliceTestDataSource.DEFAULT_HOST,
                SpliceTestDataSource.DEFAULT_PORT,
                SpliceTestDataSource.DEFAULT_USER,
                SpliceTestDataSource.DEFAULT_USER_PASSWORD,
                CLASS_NAME);
    }

    private String getDefaultURLWithTimestamp(long timestamp) {
        return getDefaultURL()+";snapshot="+timestamp;
    }

    @Test
    public void testSnapshot() throws Exception {
        long snapshot;
        try (Connection connection = DriverManager.getConnection(getDefaultURL(), new Properties())) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select count(*) from A")) {
                    assertTrue(rs.next());
                    assertEquals(2048, rs.getInt(1));
                }
                snapshot = getSnapshot(st);

                st.execute("drop table a");
                try (ResultSet rs = st.executeQuery("select count(*) from A")) {
                    fail("Table exists");
                } catch (SQLException e) {
                    assertEquals("42X05", e.getSQLState()); // table doesn't exist
                }
            }
        }
        // get connection in the past
        try (Connection connection = DriverManager.getConnection(getDefaultURLWithTimestamp(snapshot), new Properties())) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select count(*) from A")) {
                    assertTrue(rs.next());
                    assertEquals(2048, rs.getInt(1));
                }

                long returnedSnapshot = getSnapshot(st);
                assertEquals(snapshot, returnedSnapshot);

                try {
                    st.execute("drop table a");
                    fail("Could drop table");
                } catch (SQLException e) {
                    assertEquals("51045", e.getSQLState()); // table doesn't exist
                }
                try {
                    st.execute("insert into a values 2");
                    fail("Could insert into table");
                } catch (SQLException e) {
                    assertEquals("51045", e.getSQLState()); // table doesn't exist
                }
            }
        }
    }

    @Test
    public void testSnapshotWithUpdates() throws Exception {
        long original;
        long afterUpdate;
        long afterDelete;
        try (Connection connection = DriverManager.getConnection(getDefaultURL(), new Properties())) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select count(*) from B")) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                }
                original = getSnapshot(st);

                st.executeUpdate("update b set i = 2 * i");

                afterUpdate = getSnapshot(st);

                st.executeUpdate("delete from b where i < 3");

                afterDelete = getSnapshot(st);
            }
        }

        String originalResult =
                "I |\n" +
                "----\n" +
                " 0 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |";
        assertEquals(originalResult, getDataAtTimestamp(original));

        String afterUpdateResult =
                "I |\n" +
                "----\n" +
                " 0 |\n" +
                " 2 |\n" +
                " 4 |\n" +
                " 6 |";
        assertEquals(afterUpdateResult, getDataAtTimestamp(afterUpdate));

        String afterDeleteResult =
                "I |\n" +
                "----\n" +
                " 4 |\n" +
                " 6 |";
        assertEquals(afterDeleteResult, getDataAtTimestamp(afterDelete));
    }

    private long getSnapshot(Statement st) throws SQLException {
        try (ResultSet rs = st.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private String getDataAtTimestamp(long timestamp) throws Exception {
        try (Connection connection = DriverManager.getConnection(getDefaultURLWithTimestamp(timestamp), new Properties())) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select i from B")) {
                    return TestUtils.FormattedResult.ResultFactory.toString(rs);
                }
            }
        }
    }

}
