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

package com.splicemachine.derby.transactions;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

import com.splicemachine.derby.test.framework.SpliceTestDataSource;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;

/**
 * Test that requires two RegionServers running to connect to one RS, do some DDL and make sure
 * a connection to the other RS sees the appropriate state of the system.
 */
public class ClusterDDLTestIT {
    private static final String SCHEMA = ClusterDDLTestIT.class.getSimpleName().toUpperCase();


    private SpliceTestDataSource dataSource;
    
    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Before
    public void startup() {
        dataSource = new SpliceTestDataSource();
    }

    @After
    public void shutdown() {
        dataSource.shutdown();
    }

    @Test
    public void createDropTableOnTwoRegionServers() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);

        String tableName = "twoints";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(a int, b int)";
        new TableDAO(conn1).drop(SCHEMA, tableName);

        //noinspection unchecked
        new TableCreator(conn1)
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (a, b) values (?,?)", tableRef))
            .withRows(rows(
                row(1, null), row(1,15), row(1, 11), row(1, null), row(2, 25), row(2, 10)
            ))
            .create();
        String sqlText = String.format("SELECT * FROM %s ORDER BY B", tableRef);
        String expectedResult =
            "A |  B  |\n" +
                "----------\n" +
                " 2 | 10  |\n" +
                " 1 | 11  |\n" +
                " 1 | 15  |\n" +
                " 2 | 25  |\n" +
                " 1 |NULL |\n" +
                " 1 |NULL |";

        // Assure connection to RS 1 can see the table contents
        ResultSet rs = conn1.createStatement().executeQuery(sqlText);
        String conn1Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(sqlText, expectedResult, conn1Str);

        // Assure connection to RS 2 can see the table contents
        rs = conn2.createStatement().executeQuery(sqlText);
        String conn2Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        // Assure both connections see the same table contents
        assertEquals(sqlText, conn1Str, conn2Str);

        // Drop table on RS 1 and assure connection to RS 2 cannot see it any more
        new TableDAO(conn1).drop(SCHEMA, tableName);
        try {
            conn2.createStatement().executeQuery(sqlText);
        } catch (SQLException e) {
            assertEquals("Expected table not to exist.", "42X05", e.getSQLState());
        }
    }

    @Test
    public void addAndDropColumnOnTwoRegionServers() throws Exception {
        // DB-4596: alter table cache invalidation. Moved DDLUtils.flushCachesBasedOnTableDescriptor(TableDescriptor) to derby BasicDependencyManager.
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);

        String tableName = "addone";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(a int, b int)";
        new TableDAO(conn1).drop(SCHEMA, tableName);

        //noinspection unchecked
        new TableCreator(conn1)
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (a, b) values (?,?)", tableRef))
            .withRows(rows(
                row(1, null), row(1,15), row(1, 11), row(1, null), row(2, 25), row(2, 10)
            ))
            .create();
        String sqlText = String.format("SELECT * FROM %s ORDER BY B", tableRef);
        String expectedResult =
            "A |  B  |\n" +
                "----------\n" +
                " 2 | 10  |\n" +
                " 1 | 11  |\n" +
                " 1 | 15  |\n" +
                " 2 | 25  |\n" +
                " 1 |NULL |\n" +
                " 1 |NULL |";

        // Assure connection to RS 1 can see the table contents
        ResultSet rs = conn1.createStatement().executeQuery(sqlText);
        String conn1Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(sqlText, expectedResult, conn1Str);

        //
        // Add a column on RS1
        conn1.createStatement().executeUpdate(String.format("alter table %s add column d int with default 5", tableRef));

        sqlText = String.format("SELECT * FROM %s ORDER BY B", tableRef);
        expectedResult =
            "A |  B  | D |\n" +
                "--------------\n" +
                " 2 | 10  | 5 |\n" +
                " 1 | 11  | 5 |\n" +
                " 1 | 15  | 5 |\n" +
                " 2 | 25  | 5 |\n" +
                " 1 |NULL | 5 |\n" +
                " 1 |NULL | 5 |";

        // Assure connection to RS 1 can see the table contents
        rs = conn1.createStatement().executeQuery(sqlText);
        conn1Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(sqlText, expectedResult, conn1Str);

        // Assure connection to RS 2 can see the table contents
        rs = conn2.createStatement().executeQuery(sqlText);
        String conn2Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        // Assure both connections see the same table contents
        assertEquals(sqlText, conn1Str, conn2Str);

        //
        // Drop column on RS 2 and assure connection to RS 1 cannot see it any more
        conn2.createStatement().executeUpdate(String.format("alter table %s drop column a", tableRef));
        expectedResult =
            "B  | D |\n" +
                "----------\n" +
                " 10  | 5 |\n" +
                " 11  | 5 |\n" +
                " 15  | 5 |\n" +
                " 25  | 5 |\n" +
                "NULL | 5 |\n" +
                "NULL | 5 |";

        // Assure connection to RS 2 can see the table contents
        rs = conn2.createStatement().executeQuery(sqlText);
        conn2Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(sqlText, expectedResult, conn2Str);

        // Assure connection to RS 1 can see the table contents
        rs = conn1.createStatement().executeQuery(sqlText);
        conn1Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        // Assure both connections see the same table contents
        assertEquals(sqlText, conn2Str, conn1Str);
    }

    @Test
    public void testEmptyRoleOptionalCache() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn1a = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);
        try {
            conn1.createStatement().execute("drop role cddltest_zzz");
        } catch (Exception e) {}
        try {
            conn1.createStatement().execute("set role cddltest_zzz");
            Assert.assertTrue("did not throw exception",false);
        } catch (Exception e) {}
        try {
            conn2.createStatement().execute("set role cddltest_zzz");
            Assert.assertTrue("did not throw exception",false);
        } catch (Exception e) {}
        conn1.createStatement().execute("create role cddltest_zzz");
        conn1a.createStatement().execute("set role cddltest_zzz");
        conn2.createStatement().execute("set role cddltest_zzz");
    }


    //    @Test
    public void testDataSource() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);
        System.out.println("----------------------------------");
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }

        System.out.println("----------------------------------");
        conn1.close();
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }

        System.out.println("----------------------------------");
        Connection conn1a = dataSource.getConnection("localhost", 1527);
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }
    }


    @Test
    @Ignore("SPLICE-2365")
    public void testMissingTransactions() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);

        conn1.setAutoCommit(false);
        conn1.setSchema(SCHEMA);
        String tableName = "missing";
        new TableDAO(conn1).drop(SCHEMA, tableName);


        //noinspection unchecked
        new TableCreator(conn1)
                .withCreate("create table missing (i int)")
                .create();

        PreparedStatement ps = conn1.prepareStatement("insert into missing values 1");

        ps.execute();
        ResultSet rs = conn1.createStatement().executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        assertTrue(rs.next());
        long txnId = rs.getLong(1);

        for (int i = 0; i<1024; ++i) {
            ps.execute();
        }

        conn1.commit();

        byte[] rowKey = TxnUtils.getRowKey(txnId);

        Configuration config = HConfiguration.unwrapDelegate();
        Table txnTable = ConnectionFactory.createConnection(config).getTable(TableName.valueOf("splice:SPLICE_TXN"));

        Delete d = new Delete(rowKey);
        txnTable.delete(d);

        // the first region server doesn't ignore missing transactions, it should raise an error
        try {
            rs = conn1.createStatement().executeQuery("select count(*) from missing");
            fail("Expected to throw an exception");
        } catch (Exception e) {
            // ignore, expected
        }

        // The other region server ignores missing transactions, verify it works fine
        Connection conn2 = dataSource.getConnection("localhost", 1528);
        conn2.setSchema(SCHEMA);
        rs = conn2.createStatement().executeQuery("select count(*) from missing");
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertEquals(1025, count);

        // The missing transaction failure leaves conn1 in a bad state, shutdown everything manually
        try {
            dataSource.shutdown();
        } catch (Exception e) {
            //ignore
        } finally {
            dataSource = new SpliceTestDataSource();
        }
    }

}
