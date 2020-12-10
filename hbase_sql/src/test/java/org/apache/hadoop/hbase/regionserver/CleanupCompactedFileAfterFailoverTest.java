/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.compactions.SpliceCompaction;
import com.splicemachine.compactions.SpliceDefaultCompactionPolicy;
import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CleanupCompactedFileAfterFailoverTest {
    private static final Logger LOG = Logger.getLogger(CleanupCompactedFileAfterFailoverTest.class);

    private static HBaseTestingUtility TEST_UTIL;
    private static Admin admin;
    private static Table table;

    private static TableName TABLE_NAME = TableName.valueOf("CleanupCompactedFileAfterFailoverTest");
    private static byte[] ROW = Bytes.toBytes("row");
    private static byte[] FAMILY = Bytes.toBytes("cf");
    private static byte[] QUALIFIER = Bytes.toBytes("cq");
    private static byte[] VALUE = Bytes.toBytes("value");
    private static final int RS_NUMBER = 5;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        // Set the scanner lease to 20min, so the scanner can't be closed by RegionServer
        TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);
        TEST_UTIL.getConfiguration()
                .setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
        TEST_UTIL.getConfiguration().set("dfs.blocksize", "64000");
        TEST_UTIL.getConfiguration().set("dfs.namenode.fs-limits.min-block-size", "1024");
        TEST_UTIL.getConfiguration().set(TimeToLiveHFileCleaner.TTL_CONF_KEY, "0");
        Configuration config = TEST_UTIL.getConfiguration();
        config.set("hbase.coprocessor.region.classes", SIObserver.class.getCanonicalName());
        config.set(SpliceCompaction.SPLICE_SPARK_COMPACTIONS_ENABLED, "false");
        config.setInt("test.hbase.zookeeper.property.clientPort", 2181);
        config.setClass(DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY, SpliceDefaultCompactor.class, Compactor.class);
        config.setClass(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, SpliceDefaultCompactionPolicy.class, CompactionPolicy.class);

        TEST_UTIL.startMiniCluster(RS_NUMBER);
        admin = TEST_UTIL.getAdmin();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Before
    public void before() throws Exception {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
        admin.createTable(builder.build());
        TEST_UTIL.waitTableAvailable(TABLE_NAME);
        table = TEST_UTIL.getConnection().getTable(TABLE_NAME);
    }

    @After
    public void after() throws Exception {
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
    }

    @Test
    public void testCleanupAfterFailoverWithCompactOnce() throws Exception {
        testCleanupAfterFailover(1);
    }

    @Test
    public void testCleanupAfterFailoverWithCompactTwice() throws Exception {
        testCleanupAfterFailover(2);
    }

    @Test
    public void testCleanupAfterFailoverWithCompactThreeTimes() throws Exception {
        testCleanupAfterFailover(3);
    }

    private void testCleanupAfterFailover(int compactNum) throws Exception {
        HRegionServer rsServedTable = null;
        List<HRegion> regions = new ArrayList<>();
        for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
                .getLiveRegionServerThreads()) {
            HRegionServer rs = rsThread.getRegionServer();
            if (rs.getOnlineTables().contains(TABLE_NAME)) {
                regions.addAll(rs.getRegions(TABLE_NAME));
                rsServedTable = rs;
            }
        }
        assertNotNull(rsServedTable);
        assertEquals("Table should only have one region", 1, regions.size());
        HRegion region = regions.get(0);
        HStore store = region.getStore(FAMILY);

        writeDataAndFlush(3, region);
        assertEquals(3, store.getStorefilesCount());

        // Open a scanner and not close, then the storefile will be referenced
        store.getScanner(new Scan(), null, 0);

        region.compact(true);
        assertEquals(1, store.getStorefilesCount());
        // The compacted file should not be archived as there are references by user scanner
        assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());

        for (int i = 1; i < compactNum; i++) {
            // Compact again
            region.compact(true);
            assertEquals(1, store.getStorefilesCount());
            store.closeAndArchiveCompactedFiles();
            // Compacted storefiles still be 3 as the new compacted storefile was archived
            assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());
        }

        int walNum = rsServedTable.getWALs().size();
        // Roll WAL
        rsServedTable.walRoller.requestRollAll();
        // Flush again
        region.flush(true);
        // The WAL which contains compaction event marker should be archived
        assertEquals("The old WAL should be archived", walNum, rsServedTable.getWALs().size());

        rsServedTable.kill();
        // Sleep to wait failover
        Thread.sleep(3000);
        TEST_UTIL.waitTableAvailable(TABLE_NAME);

        regions.clear();
        for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
                .getLiveRegionServerThreads()) {
            HRegionServer rs = rsThread.getRegionServer();
            if (rs != rsServedTable && rs.getOnlineTables().contains(TABLE_NAME)) {
                regions.addAll(rs.getRegions(TABLE_NAME));
            }
        }
        assertEquals("Table should only have one region", 1, regions.size());
        region = regions.get(0);
        store = region.getStore(FAMILY);
        // The compacted storefile should be cleaned and only have 1 storefile
        assertEquals(1, store.getStorefilesCount());
    }

    private void writeDataAndFlush(int fileNum, HRegion region) throws Exception {
        for (int i = 0; i < fileNum; i++) {
            for (int j = 0; j < 100; j++) {
                table.put(new Put(concat(ROW, j)).addColumn(FAMILY, QUALIFIER, concat(VALUE, j)));
            }
            region.flush(true);
        }
    }

    private byte[] concat(byte[] base, int index) {
        return Bytes.toBytes(Bytes.toString(base) + "-" + index);
    }

    @Test
    public void testDeletedRowDoesntResurface() throws Exception {
        HRegionServer rsServedTable = null;
        List<HRegion> regions = new ArrayList<>();
        for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
                .getLiveRegionServerThreads()) {
            HRegionServer rs = rsThread.getRegionServer();
            if (rs.getOnlineTables().contains(TABLE_NAME)) {
                regions.addAll(rs.getRegions(TABLE_NAME));
                rsServedTable = rs;
            }
        }
        assertNotNull(rsServedTable);
        assertEquals("Table should only have one region", 1, regions.size());
        HRegion region = regions.get(0);
        HStore store = region.getStore(FAMILY);

        // Write data
        table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
        region.flush(true);
        assertEquals(1, store.getStorefilesCount());

        // Open a scanner and not close, then the storefile will be referenced
        store.getScanner(new Scan(), null, 0);

        region.compact(true);
        assertEquals(1, store.getStorefilesCount());
        // The compacted file should not be archived as there are references by user scanner
        assertEquals(1, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());


        int walNum = rsServedTable.getWALs().size();
        // Roll WAL
        rsServedTable.walRoller.requestRollAll();

        // Delete data
        table.delete(new Delete(ROW).addColumn(FAMILY, QUALIFIER));
        // And flush
        region.flush(true);
        assertEquals(2, store.getStorefilesCount());

        // Get data
        Result result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        assertTrue("The row should be deleted", result.isEmpty());

        // The WAL which contains compaction event marker should be archived
        assertEquals("The old WAL should be archived", walNum, rsServedTable.getWALs().size());

        // Open a scanner and not close, then the storefile will be referenced
        store.getScanner(new Scan(), null, 0);

        region.compact(true);
        // The compacted files should not be archived as there are references by user scanner
        assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());

        rsServedTable.kill();

        // Sleep to wait failover
        Thread.sleep(3000);
        TEST_UTIL.waitTableAvailable(TABLE_NAME);

        regions.clear();
        for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
                .getLiveRegionServerThreads()) {
            HRegionServer rs = rsThread.getRegionServer();
            if (rs != rsServedTable && rs.getOnlineTables().contains(TABLE_NAME)) {
                regions.addAll(rs.getRegions(TABLE_NAME));
            }
        }

        // Get data
        result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        assertTrue("The row should be deleted", result.isEmpty());

        assertEquals("Table should only have one region", 1, regions.size());
        region = regions.get(0);
        store = region.getStore(FAMILY);
        // The compacted storefile should be cleaned and only have 1 storefile
        assertEquals(1, store.getStorefilesCount());
    }
}