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
 *
 */

package com.splicemachine.derby.transactions;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.storage.CellType;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests transaction resolution during compactions and flushes
 */
public class TransactionResolutionIT {
    private static final String SCHEMA = TransactionResolutionIT.class.getSimpleName().toUpperCase();

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(TransactionResolutionIT.class.getSimpleName());

    public static final SpliceTableWatcher table_a = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(i int, j int)");
    public static final SpliceTableWatcher table_b = new SpliceTableWatcher("B",schemaWatcher.schemaName,"(i int, j int, primary key(i,j))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table_b)
            .around(table_a).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        Statement statement = classWatcher.getStatement();
                        statement.execute("insert into "+table_a+" (i,j) values (1,1), (2,2), (3,3), (4,4)");
                        for (int i = 0; i < 6; ++i) {
                            statement.execute("insert into a select i + (select count(*) from a), j + (select count(*) from a) from a");
                        }
                        for (int i = 0; i < 9; ++i) {
                            statement.execute("insert into a select i + (select count(*) from a), j from a");
                        }
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });


    private SpliceTestDataSource dataSource;

    @Before
    public void startup() {
        dataSource = new SpliceTestDataSource();
    }

    @After
    public void shutdown() {
        dataSource.shutdown();
    }

    @Test(timeout = 60000)
    public void testActiveTransactionResolution() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {
            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            try (Statement st = conn1.createStatement()) {
                int res = st.executeUpdate("insert into b --splice-properties useSpark=true\n" +
                        "select i, j from a order by j, i");
                assertEquals(131072, res);
                for (boolean spark : new boolean[]{true, false}) {
                    try (ResultSet rs = st.executeQuery("select count(*) from b --splice-properties useSpark="+spark)) {
                        assertTrue(rs.next());
                        int count = rs.getInt(1);
                        assertEquals(131072, count);
                    }
                }
            }
            conn1.commit();
        }
    }

    @Test
    public void testMissingTransactionsDuringFlush() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {

            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            String tableName = "missing2";
            new TableDAO(conn1).drop(SCHEMA, tableName);


            //noinspection unchecked
            new TableCreator(conn1)
                    .withCreate("create table missing2 (i int)")
                    .create();
            conn1.commit();

            try (PreparedStatement ps = conn1.prepareStatement("insert into missing2 values 1")) {

                ps.execute();
                try (Statement st = conn1.createStatement()) {
                    ResultSet rs = st.executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
                    assertTrue(rs.next());
                    long txnId = rs.getLong(1);

                    for (int i = 0; i < 1024; ++i) {
                        ps.execute();
                    }

                    conn1.commit();

                    byte[] rowKey = TxnUtils.getRowKey(txnId);

                    Configuration config = HConfiguration.unwrapDelegate();
                    Table txnTable = ConnectionFactory.createConnection(config).getTable(TableName.valueOf("splice:SPLICE_TXN"));

                    Delete d = new Delete(rowKey);
                    txnTable.delete(d);

                    long conglomerateId = TableSplit.getConglomerateId(conn1, SCHEMA, "MISSING2", null);
                    ConnectionFactory.createConnection(config).getAdmin().flush(TableName.valueOf("splice:" + conglomerateId));

                    Thread.sleep(2000);

                    rs = st.executeQuery("select count(*) from sys.systables");
                    assertTrue(rs.next());
                    int count = rs.getInt(1);
                    assertTrue(count > 10);

                }
            }

            conn1.rollback();
        }
    }

    static int getRowNums(Table table) throws IOException {
        try( ResultScanner rs = table.getScanner(new Scan())) {
            Result result;
            int count = 0;
            while ((result = rs.next()) != null) {
                count += result.size();
            }
            return count;
        }
    }

    @Test
    public void testTransactionResolutionFlush() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {

            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            String tableName = "FLUSH";
            new TableDAO(conn1).drop(SCHEMA, tableName);


            //noinspection unchecked
            new TableCreator(conn1)
                    .withCreate(String.format("create table %s (i int PRIMARY KEY)", tableName))
                    .create();
            conn1.commit();

            try (PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s values ?", tableName))) {

                for (int i = 0; i < 256; ++i) {
                    ps.setInt(1, i);
                    ps.execute();
                }

                Thread.sleep(12000); // make sure rollforward won't kick in

                conn1.commit();


                for (int i = 0; i < 256; ++i) {
                    ps.setInt(1, i+256);
                    ps.execute();
                }

                long conglomerateId = TableSplit.getConglomerateId(conn1, SCHEMA, tableName, null);

                Configuration config = HConfiguration.unwrapDelegate();
                try (org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(config)) {
                    Table table = hbaseConn.getTable(TableName.valueOf("splice:" + conglomerateId));

                    assertEquals(1024, getRowNums(table));
                    hbaseConn.getAdmin().flush(TableName.valueOf("splice:" + conglomerateId));

                    Thread.sleep(4000);

                    // Only half of the rows should be resolved
                    assertEquals(1024+256, getRowNums(table));
                }
            }
            conn1.rollback();
        }
    }

    static HashMap<CellType, Integer> getTableCellTypes(Table table) throws IOException {
        HashMap<CellType, Integer> m = new HashMap<>();
        try( ResultScanner rs = table.getScanner(new Scan())) {
            Result result;
            while ((result = rs.next()) != null) {
                while (result.advance()) {
                    CellType ct = CellUtils.getKeyValueType(result.current());
                    m.put(ct, m.getOrDefault(ct, 0) + 1);
                }
            }
            return m;
        }
    }
    @Test
    public void testTransactionResolutionCompaction() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {

            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            String tableName = "COMPACTION";
            new TableDAO(conn1).drop(SCHEMA, tableName);
            HashMap<CellType, Integer> counts;


            //noinspection unchecked
            new TableCreator(conn1)
                    .withCreate(String.format("create table %s (i int PRIMARY KEY)", tableName))
                    .create();
            conn1.commit();

            long conglomerateId = TableSplit.getConglomerateId(conn1, SCHEMA, tableName, null);
            Configuration config = HConfiguration.unwrapDelegate();
            TableName t = TableName.valueOf("splice:" + conglomerateId);

            int Ninsert1 = 100;
            int Ninsert2 = 10;

            try (PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s values ?", tableName))) {

                for (int i = 0; i < Ninsert1; ++i) {
                    ps.setInt(1, i);
                    ps.execute();
                }

                try (org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(config)) {
                    hbaseConn.getAdmin().flush(t);
                    Table table = hbaseConn.getTable(t);

                    counts = getTableCellTypes(table);
                    assertEquals( 2, counts.size() );
                    assertEquals( Ninsert1, counts.get(CellType.USER_DATA).intValue() );
                    assertEquals( Ninsert1, counts.get(CellType.FIRST_WRITE_TOKEN).intValue() );

                    Thread.sleep(1000);

                    conn1.commit();

                    for (int i = 0; i < Ninsert2; ++i) {
                        ps.setInt(1, Ninsert1+i);
                        ps.execute();
                    }

                    hbaseConn.getAdmin().flush(t);

                    counts = getTableCellTypes(table);
                    assertEquals( 2, counts.size() );
                    assertEquals( Ninsert1+Ninsert2, counts.get(CellType.USER_DATA).intValue() );
                    assertEquals( Ninsert1+Ninsert2, counts.get(CellType.FIRST_WRITE_TOKEN).intValue() );

                    hbaseConn.getAdmin().majorCompact(t);

                    try (Statement st = conn1.createStatement()) {
                        st.execute(String.format("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE( '%s', '%s')", SCHEMA, tableName));

                        Thread.sleep(2000);
                        // Only half of the rows should be resolved
                        counts = getTableCellTypes(table);
                        assertEquals( 3, counts.size() );
                        assertEquals( Ninsert1+Ninsert2, counts.get(CellType.USER_DATA).intValue() );
                        assertEquals( Ninsert1+Ninsert2, counts.get(CellType.FIRST_WRITE_TOKEN).intValue() );
                        assertEquals( Ninsert1, counts.get(CellType.COMMIT_TIMESTAMP).intValue() );
                        conn1.commit();
                        st.execute(String.format("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE( '%s', '%s')", SCHEMA, tableName));
                    }

                    Thread.sleep(2000);
                    counts = getTableCellTypes(table);
                    assertEquals( 3, counts.size() );
                    assertEquals( Ninsert1+Ninsert2, counts.get(CellType.USER_DATA).intValue() );
                    assertEquals( Ninsert1+Ninsert2, counts.get(CellType.FIRST_WRITE_TOKEN).intValue() );
                    assertEquals( Ninsert1+Ninsert2, counts.get(CellType.COMMIT_TIMESTAMP).intValue() );
                    conn1.rollback();
                }

            }
        }
    }
}
