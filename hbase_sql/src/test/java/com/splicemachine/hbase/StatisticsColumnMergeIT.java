/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 4/19/16.
 */
@Category(value = {SlowTest.class})
public class StatisticsColumnMergeIT extends SpliceUnitTest{

    private static Logger LOG = Logger.getLogger(StatisticsColumnMergeIT.class);
    public static final String CLASS_NAME = StatisticsColumnMergeIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String TABLE="T";
    private static HBaseAdmin admin;
    private static String hTableName;
    private static List<HRegionInfo> regionInfoList;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn) throws Exception {

        new TableCreator(conn)
                .withCreate("create table " + TABLE + " (c1 smallint, c2 int, c3 bigint, c4 boolean, c5 float," +
                        "c6 double, c7 decimal, c8 timestamp, c9 date, c10 varchar(10), c11 char(10), c12 int)")
                .create();

        PreparedStatement ps = methodWatcher.prepareStatement("insert into " + TABLE + "(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values(?,?,?,?,?,?,?,?,?,?,?)");
        for (int i = 0; i < 40; ++i) {
            for (int j = 0; j < 1024; j++) {
                int n = i * 1024 + j;
                ps.setShort(1, (short) n);
                ps.setInt(2, n);
                ps.setLong(3, n);
                ps.setBoolean(4, n % 2 == 0 ? true : false);
                ps.setFloat(5, n);
                ps.setDouble(6, n);
                ps.setBigDecimal(7, new BigDecimal(n));
                ps.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                ps.setDate(9, new Date(System.currentTimeMillis()));
                ps.setString(10, Long.toString(n));
                ps.setString(11, Long.toString(n));
                ps.addBatch();
            }
            ps.executeBatch();
        }

        //split the table
        Configuration config = HConfiguration.unwrapDelegate();
        admin = new HBaseAdmin(config);
        long[] conglomId = SpliceAdmin.getConglomNumbers(conn, CLASS_NAME, TABLE);
        hTableName = "splice:" + Long.toString(conglomId[0]);
        admin.split(hTableName);
        regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        long totalWaitingTime = 1000 * 60;
        long waitUnit = 2000;
        while(regionInfoList.size() == 1 && totalWaitingTime > 0) {
            totalWaitingTime -= waitUnit;
            Thread.sleep(waitUnit);
            regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        }

        ps = methodWatcher.prepareStatement("analyze schema " + CLASS_NAME);
        ps.execute();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    @Test
    public void testColumnStatsMerge() throws Exception {

        // split a region
        HRegionInfo regionInfo = regionInfoList.get(1);
        String regionName = regionInfo.getEncodedName();
        admin.split(regionName);

        regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        long totalWaitingTime = 1000 * 60;
        long waitUnit = 2000;
        while(regionInfoList.size() == 2 && totalWaitingTime > 0) {
            totalWaitingTime -= waitUnit;
            Thread.sleep(waitUnit);
            regionInfoList = admin.getTableRegions(Bytes.toBytes(hTableName));
        }

        PreparedStatement ps = methodWatcher.prepareStatement("explain select * from t a, t b where a.c2=b.c2");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertTrue(count>0);
    }
}
