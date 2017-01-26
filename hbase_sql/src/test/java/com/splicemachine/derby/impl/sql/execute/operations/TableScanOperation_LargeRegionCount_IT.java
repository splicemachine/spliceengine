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

package com.splicemachine.derby.impl.sql.execute.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.splicemachine.access.HBaseConfigurationSource;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.IntegerRows;
import com.splicemachine.test_tools.TableCreator;

/**
 * TableScanOperationIT does parallel scans for tables having more than (currently) 1 region.  Most ITs will not
 * exercise this logic as they only insert enough data for 1 region.
 *
 * SlowTest -- because splitting HTables is slow.
 *
 * Ignored -- Manual test for now unfortunately. We only switch to parallel scans after our HBaseRegionCache is
 * refreshed to reflect a large number of regions.  Workaround by manually setting the cache refresh to 1 sec before
 * starting server.  See below for details.
 */
@Category(SlowTest.class)
@Ignore("Needs to be tested manually - see javadoc")
public class TableScanOperation_LargeRegionCount_IT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = TableScanOperation_LargeRegionCount_IT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceSchemaWatcher SCHEMA_WATCHER = new SpliceSchemaWatcher(SCHEMA_NAME);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void selectFromMultipleRegions() throws Exception {
        final int ROW_COUNT = 1000 + RandomUtils.nextInt(1000);
        final int SPLIT_COUNT = 1 + RandomUtils.nextInt(33); // this line largely determines how long this test takes
        final String TABLE_NAME = "REGIONS";

        System.out.println("-------------------------------------------------------");
        System.out.println("SPLIT_COUNT=" + SPLIT_COUNT + " ROW_COUNT=" + ROW_COUNT);
        System.out.println("-------------------------------------------------------");

        //
        // After table creation HBaseRegionCache has cached a map of this tables conglomerateId to HRegionInfo
        // for one region.
        //
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table %s (a int)")
                .withInsert("insert into %s values(?)")
                .withRows(new IntegerRows(ROW_COUNT, 1))
                .withTableName(TABLE_NAME)
                .create();

        //
        // Split split split
        //
        long conglomId = methodWatcher.getConglomId(TABLE_NAME, SCHEMA_NAME);
        splitTable(conglomId, SPLIT_COUNT);

        //
        // Sleep until we are sure the HBaseRegionCache has been updated to reflect the splits.  In production
        // our scans would just be serial until the HBaseRegionCache gets updated.  For manual testing I set the cache
        // refresh period to 1 second.
        //
        Thread.sleep(2000);


        //
        // select the entire table
        //
        List<Integer> actualTableContent = methodWatcher.queryList("select a from " + TABLE_NAME);
        assertListContainsRange(actualTableContent, ROW_COUNT, 0, ROW_COUNT - 1);

        //
        // select with restrictions
        //
        actualTableContent = methodWatcher.queryList("select a from " + TABLE_NAME + " where a >= 100 and a <= " + (ROW_COUNT - 101));
        assertListContainsRange(actualTableContent, ROW_COUNT - 200, 100, ROW_COUNT - 101);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertListContainsRange(List<Integer> actualTableContent, int expectedSize, int start, int stop) {
        assertEquals(expectedSize, actualTableContent.size());
        for (int i = start; i <= stop; i++) {
            assertTrue(actualTableContent.contains(i));
        }
    }

    private void splitTable(long conglomId, int splitCount) throws Exception {
        String hbaseTableName = String.valueOf(conglomId);

        List<byte[]> rowKeys = getRowKeys(hbaseTableName);
        int step = rowKeys.size() / splitCount;
        for (int index = 0; index < rowKeys.size(); index++) {
            byte[] rowKey = rowKeys.get(index);
            Assert.fail("NOT IMPLEMENTED");
//            String rowKeyAsString = new BitFormat(false).format(rowKey);
//            if (index % step == 0 && index > 0 && index != rowKeys.size() - 1) {
//                ConglomerateUtils.splitConglomerate(conglomId,rowKey);
//                System.out.println("SPLIT AT - " + rowKeyAsString);
//            }
        }
    }

    private List<byte[]> getRowKeys(String hbaseTableName) throws IOException {
        List<byte[]> rowKeys = Lists.newArrayList();
        HTable hTable = new HTable(HConfiguration.unwrapDelegate(), hbaseTableName);
        ResultScanner resultScanner = hTable.getScanner(Bytes.toBytes("V"));
        Result result;
        while ((result = resultScanner.next()) != null) {
            rowKeys.add(result.getRow());
        }
        return rowKeys;
    }

}
