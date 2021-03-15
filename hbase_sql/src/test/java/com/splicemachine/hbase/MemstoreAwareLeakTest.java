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

package com.splicemachine.hbase;

import com.splicemachine.access.client.ClientRegionConstants;
import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
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

public class MemstoreAwareLeakTest {
    private static final Logger LOG = Logger.getLogger(MemstoreAwareLeakTest.class);

    private static HBaseTestingUtility TEST_UTIL;
    private static Admin admin;
    private static Table table;

    private static TableName TABLE_NAME = TableName.valueOf("MemstoreAwareLeakTest");
    private static byte[] ROW = Bytes.toBytes("row");
    private static byte[] FAMILY = Bytes.toBytes("cf");
    private static byte[] QUALIFIER = Bytes.toBytes("cq");
    private static byte[] VALUE = Bytes.toBytes("value");
    private static final int RS_NUMBER = 2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
        Configuration config = TEST_UTIL.getConfiguration();
        config.set("hbase.coprocessor.region.classes", MemstoreAwareObserver.class.getCanonicalName());
        config.setInt("test.hbase.zookeeper.property.clientPort", 3181);

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
    public void testNoLeakAfterStateCheckException() throws Exception {
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
        MemstoreAwareObserver mao =
                (MemstoreAwareObserver) ((CoprocessorEnvironment)region.getCoprocessorHost().findCoprocessorEnvironment(MemstoreAwareObserver.class.getName())).getInstance();

        // Region is flushing, this will trigger exception when opening Spark scanners
        mao.memstoreAware.set(new MemstoreAware(false, 0, 0, 0, true));

        Scan scan = createSparkScan();
        ResultScanner scanner = table.getScanner(scan);

        try {
            Result rs = scanner.next();
        } catch (Exception e) {
            LOG.info(e);
        }
        writeData(region);

        // we shouldn't have leaked any Scanners that kept the smallest read point behind
        assertEquals(region.getReadPoint(), region.getSmallestReadPoint());
    }

    @Test
    public void testNoLeakAfterRegionBoundaryException() throws Exception {
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
        MemstoreAwareObserver mao =
                (MemstoreAwareObserver) ((CoprocessorEnvironment)region.getCoprocessorHost().findCoprocessorEnvironment(MemstoreAwareObserver.class.getName())).getInstance();

        // Region is ok for reading
        mao.memstoreAware.set(new MemstoreAware(false, 0, 0, 0, false));

        Scan scan = createSparkScan();
        ResultScanner scanner = table.getScanner(scan);

        try {
            Result rs = scanner.next();
        } catch (Exception e) {
            LOG.info(e);
        }
        writeData(region);

        // we shouldn't have leaked any Scanners that kept the smallest read point behind
        assertEquals(region.getReadPoint(), region.getSmallestReadPoint());
    }

    private Scan createSparkScan() {
        Scan scan = new Scan();
        scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.TRUE_BYTES);
        // wrong start key
        scan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_BEGIN_KEY, SIConstants.TRUE_BYTES);
        return scan;
    }

    private void writeData(HRegion region) throws Exception {
        for (int j = 0; j < 100; j++) {
            table.put(new Put(concat(ROW, j)).addColumn(FAMILY, QUALIFIER, concat(VALUE, j)));
        }
    }
    private byte[] concat(byte[] base, int index) {
        return Bytes.toBytes(Bytes.toString(base) + "-" + index);
    }
}