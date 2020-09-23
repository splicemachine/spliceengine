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
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 3/28/16.
 */
@Category({SerialTest.class,SlowTest.class})
public class PartitionStatisticsIT {
    private static final String SCHEMA = PartitionStatisticsIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static final String TABLE = "T";
    private static TableName hTableName;
    private static List<HRegionInfo> regionInfoList;

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

        try (PreparedStatement ps = conn.prepareStatement("insert into t select * from t")) {
            for (int i = 0; i < 5; ++i) {
                ps.execute();
            }
        }

        //split the table
        long[] conglomId = SpliceAdmin.getConglomNumbers(conn, SCHEMA, TABLE);
        hTableName = TableName.valueOf("splice", Long.toString(conglomId[0]));
        syncSplit(null);
    }


    @Test
    public void testStatisticsCorrectAfterRegionSplit() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        // collect statistics
        try (PreparedStatement ps = conn.prepareStatement("analyze table t")) {
            ps.execute();
        }

        // run the query to populate partition cache
        try (PreparedStatement ps = conn.prepareStatement("explain select count(a) from t")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String s = rs.getString(1);
                }
            }
        }

        // split a region
        syncSplit(regionInfoList.get(1).getEncodedNameAsBytes());
        // collect statistics again on new partitions
        try (PreparedStatement ps = conn.prepareStatement("analyze table t")) {
            ps.execute();
        }

        // now partition cache has two paritions, make sure the query can run
        try (PreparedStatement ps = conn.prepareStatement("explain select count(a) from t")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String s = rs.getString(1);
                }
            }
        }
    }

    private static void syncSplit(byte[] regionName) throws IOException, InterruptedException {
        try (org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(HConfiguration.unwrapDelegate());
            Admin admin=connection.getAdmin()) {
            regionInfoList = admin.getTableRegions(hTableName);
            int startSize = regionInfoList.size();
            if (regionName == null) {
                admin.flush(hTableName);
                admin.split(hTableName);
            } else {
                admin.flushRegion(regionName);
                admin.splitRegion(regionName);
            }

            int s;
            do {
                Thread.sleep(200);
                regionInfoList = admin.getTableRegions(hTableName);
                s = regionInfoList.size();
                System.out.printf("origSize=%d,newSize=%d%n", startSize, s);
            } while (s == startSize);
        }
    }
}
