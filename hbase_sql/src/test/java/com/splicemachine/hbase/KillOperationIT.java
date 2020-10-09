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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by jyuan on 3/28/16.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class KillOperationIT {
    private static final Logger LOG = Logger.getLogger(KillOperationIT.class);
    private static final String SCHEMA = KillOperationIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static final String TABLE = "T";
    private static final String TABLE_PK = "P";
    private final boolean useSpark;
    private static ClusterStatus clusterStatus;
    private static org.apache.hadoop.hbase.client.Connection connection;
    private static Admin admin;


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{true},
                new Object[]{false}
        );
    }

    public KillOperationIT(boolean useSpark) {
        this.useSpark = useSpark;
    }


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
          .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);


    @BeforeClass
    public static void createTables() throws Exception {

        Connection conn = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table " + TABLE + " (a int, b int)")
              .withInsert("insert into " + TABLE + " (a, b) values (?, ?)")
              .withRows(rows(row(1, 1), row(2, 2), row(3, 3), row(4, 4)))
              .create();

        try (PreparedStatement ps = conn.prepareStatement("insert into t select a, b + ? from t")) {
            for (int i = 0; i < 10; ++i) {
                ps.setInt(1, (int) Math.pow(2, i));
                ps.execute();
            }
        }

        try (Statement s = conn.createStatement()) {
            s.execute("create table " + TABLE_PK + " (a int, b int, primary key (a,b))");
            s.execute("insert into p select * from t");
        }
    }

    @BeforeClass
    public static void initConnections() throws IOException {
        connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate());
        admin=connection.getAdmin();
        clusterStatus = admin.getClusterStatus();
    }

    @BeforeClass
    public static void killRunningOperations() throws Exception {
        TestUtils.killRunningOperations(spliceClassWatcher);
    }

    @AfterClass
    public static void dropConnections() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testNestedLoopJoinIsKilled() throws Exception {
        String sql= "select * from T a --splice-properties joinStrategy=nestedloop, useSpark="+useSpark + "\n" +
                " natural join T b --splice-properties joinStrategy=nestedloop \n" +
                " natural join T c --splice-properties joinStrategy=nestedloop \n" +
                " natural join T d --splice-properties joinStrategy=nestedloop \n" +
                " natural join T e --splice-properties joinStrategy=nestedloop \n" +
                "where a.a + b.a + c.a + d.a + e.a < 0";
        String union =  sql + " union all " + sql;
        AtomicReference<Exception> result = new AtomicReference<>();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                PreparedStatement ps = null;
                try (TestConnection connection = methodWatcher.createConnection()) {
                    ps = connection.prepareStatement(union);
                    ResultSet rs = ps.executeQuery();
                    while(rs.next()) {
                    }
                } catch (Exception e) {
                    LOG.error(e);
                    result.set(e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();

        // wait for the query to be submitted
        Thread.sleep(1000);

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        try (TestConnection connection = methodWatcher.createConnection()) {
            ResultSet opsRs = connection.query(opsCall);

            int count = 0;
            String uuid = null;
            while (opsRs.next()) {
                count++;
                if (opsRs.getString(5).equals(union)) {
                    uuid = opsRs.getString(1);
                }
            }
            assertEquals(2, count); // 2 running operations, cursor + the procedure call itself


            // kill the cursor
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('" + uuid + "')";
            connection.execute(killCall);
            LOG.info("killed operation");

            // wait for Thread termination
            thread.join();
            assertNotNull(result.get());
            Exception e = result.get();
            assertTrue(e instanceof SQLException);
            assertEquals("57014", ((SQLException) e).getSQLState());
        }
        checkReadCounts();
    }


    @Test
    public void testMultiProbeIsKilled() throws Exception {
        String sql= "select * from P a --splice-properties joinStrategy=nestedloop, useSpark="+useSpark + "\n" +
                " natural join P b --splice-properties joinStrategy=nestedloop, useSpark="+useSpark + "\n" +
                " natural join P c --splice-properties joinStrategy=nestedloop, useSpark="+useSpark + "\n" +
                " natural join P d --splice-properties joinStrategy=nestedloop, useSpark="+useSpark + "\n" +
                " where a.a in (1, 2, 3, 4)  " +
                " and b.a in (1, 2, 3, 4)  " +
                " and c.a in (1, 2, 3, 4)  " +
                " and d.a in (1, 2, 3, 4) and a.a + b.a + c.a + d.a < 0";
                String union =  sql + " union all " + sql  + " union all " + sql+ " union all " + sql+ " union all " + sql+ " union all " + sql+ " union all " + sql;
        AtomicReference<Exception> result = new AtomicReference<>();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                PreparedStatement ps = null;
                try (TestConnection connection = methodWatcher.createConnection()) {
                    ps = connection.prepareStatement(union);
                    ResultSet rs = ps.executeQuery();
                    while(rs.next()) {
                    }
                } catch (Exception e) {
                    LOG.error(e);
                    result.set(e);
                }
            }
        });
        thread.start();

        // wait for the query to be submitted
        Thread.sleep(1000);

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        try (TestConnection connection = methodWatcher.createConnection()) {
            ResultSet opsRs = connection.query(opsCall);

            int count = 0;
            String uuid = null;
            while (opsRs.next()) {
                count++;
                if (opsRs.getString(5).equals(union)) {
                    uuid = opsRs.getString(1);
                }
            }
            assertEquals(2, count); // 2 running operations, cursor + the procedure call itself


            // kill the cursor
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('" + uuid + "')";
            connection.execute(killCall);

            // wait for Thread termination
            thread.join();
            assertNotNull(result.get());
            Exception e = result.get();
            assertTrue(e instanceof SQLException);
            assertEquals("57014", ((SQLException) e).getSQLState());
        }
        checkReadCounts();
    }

    public static void checkReadCounts() throws Exception {
        int reads = 0;
        int readsNow = 0;
        for (int i = 0; i < 5; ++i) {
            LOG.info("checking counts " + i);
            Thread.sleep(5000);
            reads = getClusterReads();
            LOG.info("reads: " + reads);
            Thread.sleep(3000);
            readsNow = getClusterReads();
            LOG.info("readsnow: " + readsNow);

            if (readsNow - reads < 500) {
                LOG.info("no reads, OK");
                return;
            }

        }
        fail("Too many reads, statement not killed properly? readsNow " + readsNow + " reads " + reads);
    }


    private static int getClusterReads() throws IOException, InterruptedException {
        int reads = 0;
        for (ServerName s : clusterStatus.getServers()) {
            reads += admin.getClusterStatus().getLoad(s).getReadRequestsCount();
            LOG.info("partial reads " + reads + " from " + s);
        }

        return reads;
    }
}
