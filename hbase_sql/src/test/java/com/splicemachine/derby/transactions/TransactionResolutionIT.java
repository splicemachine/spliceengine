/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTestDataSource;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests transaction resolution during compactions and flushes
 */
public class TransactionResolutionIT {
    private static final String SCHEMA = TransactionResolutionIT.class.getSimpleName().toUpperCase();


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

    @Test
    public void testTransactionResolutionFlush() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {

            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            String tableName = "FLUSH";
            new TableDAO(conn1).drop(SCHEMA, tableName);


            //noinspection unchecked
            new TableCreator(conn1)
                    .withCreate(String.format("create table %s (i int)", tableName))
                    .create();
            conn1.commit();

            try (PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s values 1", tableName))) {

                for (int i = 0; i < 256; ++i) {
                    ps.execute();
                }

                conn1.commit();


                for (int i = 0; i < 256; ++i) {
                    ps.execute();
                }

                long conglomerateId = TableSplit.getConglomerateId(conn1, SCHEMA, tableName, null);

                Configuration config = HConfiguration.unwrapDelegate();
                try (org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(config)) {
                    Table table = hbaseConn.getTable(TableName.valueOf("splice:" + conglomerateId));

                    Scan scan = new Scan();
                    try (ResultScanner rs = table.getScanner(scan)) {

                        Result result;
                        int count = 0;
                        while ((result = rs.next()) != null) {
                            count += result.size();
                        }
                        assertEquals(512, count);
                    }


                    hbaseConn.getAdmin().flush(TableName.valueOf("splice:" + conglomerateId));

                    Thread.sleep(2000);
                    try (ResultScanner rs = table.getScanner(scan)) {

                        Result result;
                        int count = 0;
                        while ((result = rs.next()) != null) {
                            count += result.size();
                        }
                        // Only half of the rows should be resolved
                        assertEquals(512 + 256, count);
                    }
                }
            }
            conn1.rollback();
        }
    }

    @Test
    public void testTransactionResolutionCompaction() throws Exception {
        try (Connection conn1 = dataSource.getConnection("localhost", 1527)) {

            conn1.setAutoCommit(false);
            conn1.setSchema(SCHEMA);
            String tableName = "COMPACTION";
            new TableDAO(conn1).drop(SCHEMA, tableName);


            //noinspection unchecked
            new TableCreator(conn1)
                    .withCreate(String.format("create table %s (i int)", tableName))
                    .create();
            conn1.commit();

            try (PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s values 1", tableName))) {

                for (int i = 0; i < 256; ++i) {
                    ps.execute();
                }

                long conglomerateId = TableSplit.getConglomerateId(conn1, SCHEMA, tableName, null);
                Configuration config = HConfiguration.unwrapDelegate();
                try (org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(config)) {
                    hbaseConn.getAdmin().flush(TableName.valueOf("splice:" + conglomerateId));

                    Thread.sleep(1000);

                    conn1.commit();

                    for (int i = 0; i < 256; ++i) {
                        ps.execute();
                    }

                    hbaseConn.getAdmin().flush(TableName.valueOf("splice:" + conglomerateId));
                    Table table = hbaseConn.getTable(TableName.valueOf("splice:" + conglomerateId));
                    Scan scan = new Scan();
                    try (ResultScanner rs = table.getScanner(scan)) {

                        Result result;
                        int count = 0;
                        while ((result = rs.next()) != null) {
                            count += result.size();
                        }
                        assertEquals(512, count);
                    }

                    hbaseConn.getAdmin().majorCompact(TableName.valueOf("splice:" + conglomerateId));

                    try (Statement st = conn1.createStatement()) {
                        st.execute(String.format("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE( '%s', '%s')", SCHEMA, tableName));

                        Thread.sleep(2000);
                        try (ResultScanner rs = table.getScanner(scan)) {

                            Result result;
                            int count = 0;
                            while ((result = rs.next()) != null) {
                                count += result.size();
                            }
                            // Only half of the rows should be resolved
                            assertEquals(512 + 256, count);
                        }

                        conn1.commit();
                        st.execute(String.format("call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE( '%s', '%s')", SCHEMA, tableName));
                    }

                    Thread.sleep(2000);
                    try (ResultScanner rs = table.getScanner(scan)) {

                        Result result;
                        int count = 0;
                        while ((result = rs.next()) != null) {
                            count += result.size();
                        }
                        // All rows should be resolved
                        assertEquals(512 * 2, count);
                    }
                    conn1.rollback();
                }

            }
        }
    }
}
