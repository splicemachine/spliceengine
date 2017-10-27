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
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 8/15/17.
 */
public class SpliceRegionAdminIT {
    private static final String SCHEMA_NAME = SpliceRegionAdminIT.class.getSimpleName().toUpperCase();
    private static final String LINEITEM = "LINEITEM";
    private static final String ORDERS = "ORDERS";
    private static final String SHIPDATE_IDX = "L_SHIPDATE_IDX";
    private static final String CUST_IDX = "O_CUST_IDX";
    private static List<String> spliceTableSplitKeys = Lists.newArrayList();
    private static List<String> hbaseTableSplitKeys = Lists.newArrayList();
    private static List<String> spliceIndexSplitKeys = Lists.newArrayList();
    private static List<String> hbaseIndexSplitKeys = Lists.newArrayList();
    
    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void init() throws Exception {
        TestUtils.executeSqlFile(spliceClassWatcher, "tcph/TPCHIT.sql", SCHEMA_NAME);
        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX('%s','%s',null,'L_ORDERKEY,L_LINENUMBER'," +
                        "'%s','|',null,null,null,null,-1,'/BAD',true,null)",SCHEMA_NAME,LINEITEM,
                getResource("lineitemKey.csv")));

        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX('%s','%s','O_CUST_IDX'," +
                        "'O_CUSTKEY,O_ORDERKEY'," +
                        "'%s','|',null,null,null,null,-1,'/BAD',true,null)",SCHEMA_NAME,ORDERS,
                getResource("custIndex.csv")));
        spliceClassWatcher.execute(String.format("CALL SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', true)", SCHEMA_NAME, LINEITEM, getResource("lineitem.tbl"), getResource("data")));

        spliceTableSplitKeys.add(0, "{ NULL, NULL }");
        spliceTableSplitKeys.add(1, "{ 1, NULL }");
        spliceTableSplitKeys.add(2, "{ 1, 2 }");
        spliceTableSplitKeys.add(3, "{ 1, 4 }");
        spliceTableSplitKeys.add(4, "{ 1, 6 }");
        spliceTableSplitKeys.add(5, "{ 1, 8 }");
        spliceTableSplitKeys.add(6, "{ 2, NULL }");
        spliceTableSplitKeys.add(7, "{ 2, 2 }");

        hbaseTableSplitKeys.add(0, "");
        hbaseTableSplitKeys.add(1, "\\x81");
        hbaseTableSplitKeys.add(2, "\\x81\\x00\\x82");
        hbaseTableSplitKeys.add(3, "\\x81\\x00\\x84");
        hbaseTableSplitKeys.add(4, "\\x81\\x00\\x86");
        hbaseTableSplitKeys.add(5, "\\x81\\x00\\x88");
        hbaseTableSplitKeys.add(6, "\\x82");
        hbaseTableSplitKeys.add(7, "\\x82\\x00\\x82");

        spliceIndexSplitKeys.add(0, "{ NULL, NULL }");
        spliceIndexSplitKeys.add(1, "{ 75857, 3968900 }");

        hbaseIndexSplitKeys.add(0, "");
        hbaseIndexSplitKeys.add(1, "\\xE1(Q\\x00\\xE4<\\x8F\\x84");
    }

    @Test
    public void testTable() throws Exception {

        Connection connection = methodWatcher.getOrCreateConnection();

        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        long conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, LINEITEM, null);
        TableName tn = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tn.getName());
        for (HRegionInfo partition : partitions) {
            String startKey = Bytes.toStringBinary(partition.getStartKey());
            int index = Collections.binarySearch(hbaseTableSplitKeys, startKey);
            String encodedRegionName = partition.getEncodedName();
            PreparedStatement ps = methodWatcher.prepareStatement("CALL SYSCS_UTIL.GET_START_KEY(?,?,null,?)");
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, encodedRegionName);

            ResultSet rs = ps.executeQuery();
            rs.next();
            String result = rs.getString(1);
            Assert.assertEquals(result, spliceTableSplitKeys.get(index));
        }
    }

    @Test
    public void testIndex() throws Exception {

        Connection connection = methodWatcher.getOrCreateConnection();
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        long conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, ORDERS, CUST_IDX);
        TableName tn = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tn.getName());
        for (HRegionInfo partition : partitions) {
            String startKey = Bytes.toStringBinary(partition.getStartKey());
            int index = Collections.binarySearch(hbaseIndexSplitKeys, startKey);
            String encodedRegionName = partition.getEncodedName();
            PreparedStatement ps = methodWatcher.prepareStatement("CALL SYSCS_UTIL.GET_START_KEY(?,?,?,?)");
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, ORDERS);
            ps.setString(3, CUST_IDX);
            ps.setString(4, encodedRegionName);

            ResultSet rs = ps.executeQuery();
            rs.next();
            String result = rs.getString(1);
            Assert.assertEquals(result, spliceIndexSplitKeys.get(index));
        }
    }

    @Test
    public void testListTableRegions() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(String.format("call syscs_util.get_regions('%s', '%s', null, null, null,'|', null,null,null,null)", SCHEMA_NAME, LINEITEM));
        int i = 0;
        while(rs.next()) {
            String spliceStartKey = rs.getString("SPLICE_START_KEY");
            String spliceEndKey = rs.getString("SPLICE_END_KEY");
            String hbaseStartKey = rs.getString("HBASE_START_KEY");
            String hbaseEndKey = rs.getString("HBASE_END_KEY");
            Assert.assertEquals(spliceTableSplitKeys.get(i), spliceStartKey);
            Assert.assertEquals(hbaseTableSplitKeys.get(i), hbaseStartKey);
            if (i < spliceTableSplitKeys.size() -1) {
                Assert.assertEquals(spliceTableSplitKeys.get(i+1), spliceEndKey);
                Assert.assertEquals(hbaseTableSplitKeys.get(i+1), hbaseEndKey);
            }
            i++;
        }

    }

    @Test
    public void testListIndexRegions() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(String.format("call syscs_util.get_regions('%s', '%s', '%s', null, null,'|', null,null,null,null)", SCHEMA_NAME, ORDERS, CUST_IDX));
        int i = 0;
        while(rs.next()) {
            String spliceStartKey = rs.getString("SPLICE_START_KEY");
            String spliceEndKey = rs.getString("SPLICE_END_KEY");
            String hbaseStartKey = rs.getString("HBASE_START_KEY");
            String hbaseEndKey = rs.getString("HBASE_END_KEY");
            Assert.assertEquals(spliceIndexSplitKeys.get(i), spliceStartKey);
            Assert.assertEquals(hbaseIndexSplitKeys.get(i), hbaseStartKey);
            if (i < spliceIndexSplitKeys.size() -1) {
                Assert.assertEquals(spliceIndexSplitKeys.get(i+1), spliceEndKey);
                Assert.assertEquals(hbaseIndexSplitKeys.get(i+1), hbaseEndKey);
            }
            i++;
        }

    }

    @Test
    public void negativeTests() throws Exception {

        String sql = "CALL SYSCS_UTIL.GET_ENCODED_REGION_NAME(?, ?, ?,'1|2', '|', null, null, null, null)";
        PreparedStatement ps = methodWatcher.prepareStatement(sql);

        try {
            ps.setString(1, "NOTEXIST");
            ps.setString(2, LINEITEM);
            ps.setString(3, null);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
        }

        try{
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, "NOTEXIST");
            ps.setString(3, null);
            ps.execute();
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NOT_FOUND.substring(0,5));
        }

        try{
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, "NOTEXIST");
            ps.execute();
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.LANG_INDEX_NOT_FOUND);
        }

        sql = "CALL SYSCS_UTIL.GET_START_KEY(?, ?, ?, ?)";
        ps = methodWatcher.prepareStatement(sql);

        try {
            ps.setString(1, "NOTEXIST");
            ps.setString(2, LINEITEM);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region");
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.LANG_SCHEMA_DOES_NOT_EXIST);
        }

        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, "NOTEXIST");
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region");
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NOT_FOUND.substring(0,5));
        }

        try{
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, "NOTEXIST");
            ps.setString(4, "region");
            ps.execute();
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.LANG_INDEX_NOT_FOUND );
        }

        try{
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region");
            ps.execute();
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.REGION_DOESNOT_EXIST);
        }
    }

    private static String getResource(String name) {
        return SpliceUnitTest.getResourceDirectory() + "tcph/data/" + name;
    }
}
