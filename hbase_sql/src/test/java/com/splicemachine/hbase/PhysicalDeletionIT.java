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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 5/24/17.
 */
public class PhysicalDeletionIT extends SpliceUnitTest {

    private static final String NAME = PhysicalDeletionIT.class.getSimpleName().toUpperCase();
    private static final String SCHEMA1 = NAME + "1";
    private static final String SCHEMA2 = NAME + "2";
    private static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA1);
    private static final SpliceSchemaWatcher schema1Watcher = new SpliceSchemaWatcher(SCHEMA1);
    private static final SpliceSchemaWatcher schema2Watcher = new SpliceSchemaWatcher(SCHEMA2);
    private static final String MRP11 = "MRP11";
    private static final String MRP21 = "MRP21";
    private static final String MRP22 = "MRP22";
    @ClassRule
    public static  TestRule chain =
            RuleChain.outerRule(classWatcher)
                    .around(schema1Watcher)
                    .around(schema2Watcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA1);

    @BeforeClass
    public static void createTables() throws Exception {
        TestConnection conn = classWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table A (i int, j int, primary key(i))")
                .withInsert("insert into A values(?, ?)")
                .withRows(rows(
                        row(1, 1),
                        row(2, 2),
                        row(3, 3),
                        row(4, 4)))
                .create();
        new TableCreator(conn).withCreate(String.format("create table %s.%s(col1 int)", SCHEMA1, MRP11)).create();
        new TableCreator(conn).withCreate(String.format("create table %s.%s(col1 int)", SCHEMA2, MRP21)).create();
        new TableCreator(conn).withCreate(String.format("create table %s.%s(col1 int)", SCHEMA2, MRP22)).create();
    }

    private void assertPurgeDeletedRowsFlag(boolean expected) throws SQLException {
        String sql = "select purge_deleted_rows from sys.systables t, sys.sysschemas s where tablename='A' and " +
                "schemaname='" + SCHEMA1 + "' and t.schemaid=s.schemaid";
        ResultSet rs = methodWatcher.executeQuery(sql);
        boolean isValid = rs.next();
        Assert.assertTrue(isValid);
        boolean purgeDeletedRows = rs.getBoolean(1);
        Assert.assertEquals(expected, purgeDeletedRows);
    }

    @Test
    public void testPhysicalDelete() throws Exception {

        TestConnection conn = classWatcher.getOrCreateConnection();

        assertPurgeDeletedRowsFlag(false);

        methodWatcher.executeUpdate("delete from A");
        methodWatcher.executeUpdate("insert into a values(1,1), (2,2)");

        Thread.sleep(2000); // wait for commit markers to be written

        try (Connection connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate())) {
            methodWatcher.execute("CALL SYSCS_UTIL.SYSCS_FLUSH_TABLE('" + SCHEMA1 + "','A')");
            methodWatcher.execute("CALL SYSCS_UTIL.SET_PURGE_DELETED_ROWS('" + SCHEMA1 + "','A',true)");
            methodWatcher.execute("CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('" + SCHEMA1 + "','A')");

            assertPurgeDeletedRowsFlag(true);

            Scan s = new Scan();
            long[] conglomId = SpliceAdmin.getConglomNumbers(conn, SCHEMA1, "A");
            TableName hTableName = TableName.valueOf("splice:" + conglomId[0]);
            Table table = connection.getTable(hTableName);
            ResultScanner scanner = table.getScanner(s);
            int count = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                System.out.println("RAW CELLS: " + Arrays.toString(rr.rawCells()));
                count++;
            }
            Assert.assertEquals(2, count);
        }
    }

    private void setMinRetentionPeriodOf(String schema, String table, int minRetentionPeriod) throws Exception {
        if(table == null) {
            methodWatcher.execute("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('" + schema + "', NULL, " + minRetentionPeriod + ")");
        } else {
            methodWatcher.execute("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('" + schema + "', '" + table + "', " + minRetentionPeriod + ")");
        }
    }

    private int getMinRetentionPeriodOf(String table) throws SQLException {
        ResultSet rs = methodWatcher.executeQuery("SELECT MIN_RETENTION_PERIOD FROM SYS.SYSTABLES WHERE TABLENAME = '" + table + "'");
        Assert.assertTrue(rs.next());
        return rs.getInt(1);
    }

    @Test
    public void testMinRetentionPeriod() throws Exception {
        Assert.assertEquals(0, getMinRetentionPeriodOf(MRP11));
        Assert.assertEquals(0, getMinRetentionPeriodOf(MRP21));
        Assert.assertEquals(0, getMinRetentionPeriodOf(MRP22));

        setMinRetentionPeriodOf(SCHEMA1, MRP11, 200);
        Assert.assertEquals(200, getMinRetentionPeriodOf(MRP11));

        setMinRetentionPeriodOf(SCHEMA2, null, 400);
        Assert.assertEquals(400, getMinRetentionPeriodOf(MRP21));
        Assert.assertEquals(400, getMinRetentionPeriodOf(MRP22));
    }

    @Test
    public void testSettingMinRetentionPeriodToInvalidTableThrows() throws Exception {
        try {
            setMinRetentionPeriodOf(SCHEMA1, "NON_EXISTING_TABLE", 400);
            Assert.fail("expected exception to be thrown");
        } catch (Exception e) {
            Assert.assertEquals("Table 'NON_EXISTING_TABLE' does not exist.  ", e.getMessage());
        }
    }

    @Test
    public void testSettingMinRetentionPeriodToInvalidSchemaThrows() throws Exception {
        try {
            setMinRetentionPeriodOf("INVALID_SCHEMA", "NON_EXISTING_TABLE", 400);
            Assert.fail("expected exception to be thrown");
        } catch (Exception e) {
            Assert.assertEquals("Schema 'INVALID_SCHEMA' does not exist", e.getMessage());
        }
    }

    @Test
    public void testSettingMinRetentionPeriodToInvalidMinRetentionPeriodThrows() throws Exception {
        try {
            setMinRetentionPeriodOf(SCHEMA1, MRP11, -400);
            Assert.fail("expected exception to be thrown");
        } catch (Exception e) {
            Assert.assertEquals("'-400' is not in the valid range 'non-negative number'.", e.getMessage());
        }
    }
}
