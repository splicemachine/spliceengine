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
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import splice.com.google.common.collect.Lists;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 8/15/17.
 */
public class SpliceRegionAdminIT {
    private static final String SCHEMA_NAME = SpliceRegionAdminIT.class.getSimpleName().toUpperCase();
    private static final String LINEITEM = "LINEITEM";
    private static final String ORDERS = "ORDERS";
    private static final String SHIPDATE_IDX = "L_SHIPDATE_IDX";
    private static final String CUST_IDX = "O_CUST_IDX";
    private static final String A = "A";
    private static final String AI = "AI";
    private static final String B = "B";
    private static final String BI = "BI";
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
        String badDir = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME).getCanonicalPath();
        TestUtils.executeSqlFile(spliceClassWatcher, "tcph/TPCHIT.sql", SCHEMA_NAME);
        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX('%s','%s',null,'L_ORDERKEY,L_LINENUMBER'," +
                        "'%s','|',null,null,null,null,-1,'%s',true,null)",SCHEMA_NAME,LINEITEM,
                getResource("lineitemKey.csv"), badDir));

        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX('%s','%s','O_CUST_IDX'," +
                        "'O_CUSTKEY,O_ORDERKEY'," +
                        "'%s','|',null,null,null,null,-1,'%s',true,null)",SCHEMA_NAME,ORDERS,
                getResource("custIndex.csv"), badDir));
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
        hbaseTableSplitKeys.add(1, "\\x81\\x00");
        hbaseTableSplitKeys.add(2, "\\x81\\x00\\x82");
        hbaseTableSplitKeys.add(3, "\\x81\\x00\\x84");
        hbaseTableSplitKeys.add(4, "\\x81\\x00\\x86");
        hbaseTableSplitKeys.add(5, "\\x81\\x00\\x88");
        hbaseTableSplitKeys.add(6, "\\x82\\x00");
        hbaseTableSplitKeys.add(7, "\\x82\\x00\\x82");

        spliceIndexSplitKeys.add(0, "{ NULL, NULL }");
        spliceIndexSplitKeys.add(1, "{ 75857, 3968900 }");

        hbaseIndexSplitKeys.add(0, "");
        hbaseIndexSplitKeys.add(1, "\\xE1(Q\\x00\\xE4<\\x8F\\x84");

        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table A (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into A values(?,?,?)")
                .withIndex("create index AI on A(a)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5)))
                .create();
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'A', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'A', 'AI','\\x83')");

        new TableCreator(conn)
                .withCreate("create table B (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into B values(?,?,?)")
                .withIndex("create index BI on B(a)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5)))
                .create();
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'B', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'B', 'BI','\\x83')");

        new TableCreator(conn)
                .withCreate("create table C (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into C values(?,?,?)")
                .withIndex("create index CI on C(a)")
                .create();
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'C', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'C', 'CI','\\x83')");

        new TableCreator(conn)
                .withCreate("create table D (a int, b int, c int, primary key(a,b))")
                .create();
        for (int i = 80; i <100; ++i) {
            String split = String.format("call syscs_util.syscs_split_table_or_index_at_points('SPLICEREGIONADMINIT', 'D', null,'\\x%d')", i);
            spliceClassWatcher.execute(split);
        }
    }

    @Test
    public void testTable() throws Exception {

        Connection connection = methodWatcher.getOrCreateConnection();

        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        long conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, LINEITEM, null);
        TableName tn = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tn);
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
        List<HRegionInfo> partitions = admin.getTableRegions(tn);
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
    public void testMultipleMerge() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.get_regions('SPLICEREGIONADMINIT', 'D', null, null, null, null, null,null,null,null)");
        List<String> regions = new ArrayList<>();
        while (rs.next()) {
            regions.add(rs.getString("ENCODED_REGION_NAME"));
        }

        for (int i = 1; i < regions.size(); i++) {
            String delete = String.format("call syscs_util.delete_region('SPLICEREGIONADMINIT', 'D', null, '%s', true)", regions.get(i));
            methodWatcher.execute(delete);
        }
    }
    @Test(timeout = 240000)
    public void testDeleteAndMergeRegion() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        long conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, A, null);
        TableName tableName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tableName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                methodWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', null, '%s', true)",
                                SCHEMA_NAME, A, encodedRegionName));
                break;
            }
        }
        while (partitions.size() != 1) {
            Thread.sleep(1000);
            partitions = admin.getTableRegions(tableName);
        }

        conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, A, AI);
        TableName indexName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        partitions = admin.getTableRegions(indexName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                methodWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', '%s', '%s', true)",
                                SCHEMA_NAME, A, AI, encodedRegionName));
                break;
            }
        }

        while (partitions.size() != 1) {
            Thread.sleep(1000);
            partitions = admin.getTableRegions(indexName);
        }
        int expected = 2;
        String sql = "select count(*) from %s --SPLICE-PROPERTIES index=%s";
        ResultSet rs = methodWatcher.executeQuery(String.format(sql, A, "null"));
        rs.next();
        Assert.assertEquals(expected, rs.getInt(1));

        rs = methodWatcher.executeQuery(String.format(sql, A, AI));
        rs.next();
        Assert.assertEquals(expected, rs.getInt(1));
    }

    @Test
    public void testDeleteRegion() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        long conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, B, null);
        TableName tableName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tableName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                methodWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', null, '%s', false)",
                        SCHEMA_NAME, B, encodedRegionName));
                break;
            }
        }

        conglomerateId = TableSplit.getConglomerateId(connection, SCHEMA_NAME, B, BI);
        TableName indexName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        partitions = admin.getTableRegions(indexName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                methodWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', '%s', '%s', false)",
                        SCHEMA_NAME, B, BI, encodedRegionName));
                break;
            }
        }

        int expected = 2;
        String sql = "select count(*) from %s --SPLICE-PROPERTIES index=%s";
        ResultSet rs = methodWatcher.executeQuery(String.format(sql, B, "null"));
        rs.next();
        Assert.assertEquals(expected, rs.getInt(1));

        rs = methodWatcher.executeQuery(String.format(sql, B, BI));
        rs.next();
        Assert.assertEquals(expected, rs.getInt(1));
    }

    @Test
    public void testCompaction() throws Exception {
        String flush = "call syscs_util.syscs_flush_table('SPLICEREGIONADMINIT', 'C')";
        String[] regionName = new String[2];
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.get_regions('SPLICEREGIONADMINIT', 'C', null, null, null, null, null, null, null, null)");
        int i = 0;
        while (rs.next()) {
            regionName[i++] = rs.getString("ENCODED_REGION_NAME");
        }
        methodWatcher.execute("insert into c values(1,1,1)");
        methodWatcher.execute(flush);
        methodWatcher.execute("insert into c values(2,2,2)");
        methodWatcher.execute(flush);
        methodWatcher.execute("insert into c values(4,4,4)");
        methodWatcher.execute(flush);
        methodWatcher.execute("insert into c values(5,5,5)");
        methodWatcher.execute(flush);

        rs = methodWatcher.executeQuery("call syscs_util.get_regions('SPLICEREGIONADMINIT', 'C', null, null, null, null, null, null, null, null)");
        while (rs.next()) {
            int numFile = rs.getInt("NUM_HFILES");
            Assert.assertTrue(numFile > 1);
        }
        // major compact the 2nd region
        String majorCompaction = String.format("call syscs_util.major_compact_region('SPLICEREGIONADMINIT', 'C', null, '%s')", regionName[1]);
        methodWatcher.execute(majorCompaction);

        rs = methodWatcher.executeQuery("call syscs_util.get_regions('SPLICEREGIONADMINIT', 'C', null, null, null, null, null, null, null, null)");
        i = 0;
        while (rs.next()) {
            int numFile = rs.getInt("NUM_HFILES");
            if ( i == 0) {
                // The first region should have more than 1 file.
                Assert.assertTrue(numFile > 1);
            }
            else {
                // The second region should only have 1 file
                //Assert.assertTrue(numFile == 1);
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

        try{
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, null);
            ps.setString(3, null);
            ps.execute();
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5));
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

        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, null);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region");
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5));
        }

        sql = "CALL SYSCS_UTIL.COMPACT_REGION(?, ?, ?, ?)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, null);
            ps.setString(2, null);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region");
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5));
        }

        sql = "CALL SYSCS_UTIL.COMPACT_REGION(?, ?, ?, ?)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, null);
            ps.setString(2, LINEITEM);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, null);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.PARAMETER_CANNOT_BE_NULL.substring(0,5));
        }

        sql = "CALL SYSCS_UTIL.GET_REGIONS(?, ?, ?, 'start', 'end', '|', null, null, null, null)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, null);
            ps.setString(2, null);
            ps.setString(3, SHIPDATE_IDX);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5));
        }

        sql = "CALL SYSCS_UTIL.MERGE_REGIONS(?, ?, ?, ?, ?)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, null);
            ps.setString(3, SHIPDATE_IDX);
            ps.setString(4, "region1");
            ps.setString(5, "region2");
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5));
        }

        sql = "CALL SYSCS_UTIL.GET_ENCODED_REGION_NAME(?, ?, ?, null, '|', null, null, null, null)";
        ps = methodWatcher.prepareStatement(sql);

        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, null);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.PARAMETER_CANNOT_BE_NULL, sqlcode);
        }

        sql = "CALL SYSCS_UTIL.GET_START_KEY(?,?,null,?)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.setString(3, null);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.PARAMETER_CANNOT_BE_NULL, sqlcode);
        }

        sql = "CALL SYSCS_UTIL.DELETE_REGION(?,?,null,null, true)";
        ps = methodWatcher.prepareStatement(sql);
        try {
            ps.setString(1, SCHEMA_NAME);
            ps.setString(2, LINEITEM);
            ps.execute();
        }
        catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.PARAMETER_CANNOT_BE_NULL, sqlcode);
        }

    }

    private static String getResource(String name) {
        return SpliceUnitTest.getResourceDirectory() + "tcph/data/" + name;
    }
}
