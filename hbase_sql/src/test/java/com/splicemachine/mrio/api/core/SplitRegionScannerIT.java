/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.SamplingFilter;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SplitRegionScanner;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class SplitRegionScannerIT  extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SplitRegionScannerIT.class);
    private static final String SCHEMA = SplitRegionScannerIT.class.getSimpleName();
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherC = new SpliceTableWatcher("C",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherD = new SpliceTableWatcher("D",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherE = new SpliceTableWatcher("E",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherF = new SpliceTableWatcher("F",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherG = new SpliceTableWatcher("G",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherH = new SpliceTableWatcher("H",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherI = new SpliceTableWatcher("I",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherJ = new SpliceTableWatcher("J",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherK = new SpliceTableWatcher("K",SCHEMA,"(col1 int, col2 varchar(56), primary key (col1))");

    protected static final long ITERATIONS = 20000;
    protected static SIDriver driver;
    protected static SConfiguration config;
    protected static HBaseConnectionFactory instance;
    protected static Clock clock;
    protected static TreeSet simpleScan;

    static {
        //boot SI components
        try {
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
            driver = env.getSIDriver();
            config=driver.getConfiguration();
            instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
            clock = driver.getClock();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcherA)
		.around(spliceTableWatcherB)
            .around(spliceTableWatcherC)
            .around(spliceTableWatcherD)
            .around(spliceTableWatcherE)
            .around(spliceTableWatcherF)
            .around(spliceTableWatcherG)
            .around(spliceTableWatcherH)
            .around(spliceTableWatcherI)
            .around(spliceTableWatcherJ)
            .around(spliceTableWatcherK)

		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SCHEMA + ".A (col1,col2) values (?,?)");
					for (int i = 0; i< ITERATIONS; i++) {
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
                        psA.executeUpdate();
					}
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                    ,SCHEMA + ".B",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                    ,SCHEMA + ".C",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".D",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".E",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".F",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".G",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".H",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".I",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".J",SCHEMA + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SCHEMA + ".K",SCHEMA + ".A"));
                } catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	@Test
	public void simpleScan() throws SQLException, IOException, InterruptedException {
        Partition partition = driver.getTableFactory()
                .getTable(sqlUtil.getConglomID(spliceTableWatcherA.toString()));
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        List<Cell> newCells = new ArrayList<>();
        simpleScan = new TreeSet();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                simpleScan.add(CellUtils.toHex(CellUtil.cloneRow(newCells.get(0))));
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);
	}

    @Test
    public void simpleScanPostFlush() throws SQLException, IOException, InterruptedException {
        Partition partition = driver.getTableFactory()
                .getTable(sqlUtil.getConglomID(spliceTableWatcherB.toString()));
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        List<Cell> newCells = new ArrayList<>();
        partition.flush();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);


    }

    @Test
    public void simpleScanPostSplit() throws SQLException, IOException, InterruptedException {
        String tableName = sqlUtil.getConglomID(spliceTableWatcherC.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
        Scan scan = new Scan();
        SplitRegionScanner srs = new SplitRegionScanner(scan,
                htable,
                clock,partition, driver.getConfiguration(), htable.getConfiguration());
        while (srs.next(newCells)) {
            i++;
            newCells.clear();
        }
        srs.close();
        htable.close();
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);
    }

    @Test
    public void simpleMergeTest() throws Exception {
        // Test Records at end
        spliceClassWatcher.executeUpdate(String.format("insert into %s select col1+" + ITERATIONS+", col2 from %s"
                ,SCHEMA + ".D",SCHEMA + ".A"));
        spliceClassWatcher.executeUpdate(String.format("insert into %s values (-1,'foo')" // Test A Record before
                ,SCHEMA + ".D"));

        String tableName = sqlUtil.getConglomID(spliceTableWatcherD.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",2*ITERATIONS+1,i);
    }

    @Test
    public void simpleMergeWithConcurrentSplitTest() throws Exception {
        spliceClassWatcher.executeUpdate(String.format("insert into %s select col1+" + ITERATIONS+", col2 from %s"
                ,SCHEMA + ".E",SCHEMA + ".A"));
        String tableName = sqlUtil.getConglomID(spliceTableWatcherE.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                if (i==ITERATIONS/2)
                    driver.getTableFactory().getAdmin().splitTable(tableName);
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",2*ITERATIONS,i);
    }

    @Test
    public void simpleMergeWithConcurrentFlushTest() throws Exception {
        spliceClassWatcher.executeUpdate(String.format("insert into %s select col1+" + ITERATIONS+", col2 from %s"
                ,SCHEMA + ".F",SCHEMA + ".A"));
        String tableName = sqlUtil.getConglomID(spliceTableWatcherF.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                if (i==ITERATIONS/2)
                    partition.flush();
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",2*ITERATIONS,i);
    }

    @Test
    public void simpleMergeWithConcurrentFlushAndSplitTest() throws Exception {
        // Currently fails with:
        // org.apache.hadoop.hbase.DoNotRetryIOException: org.apache.hadoop.hbase.DoNotRetryIOException
        // at com.splicemachine.hbase.MemstoreAwareObserver.preStoreScannerOpen(MemstoreAwareObserver.java:159)
        // at org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost$51.call(RegionCoprocessorHost.java:1291)

        String tableName = sqlUtil.getConglomID(spliceTableWatcherJ.toString());
        Partition partition = driver.getTableFactory()
                                    .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        List<Cell> newCells = new ArrayList<>();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                                                            htable,
                                                            clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                if (i==ITERATIONS/2) {
                    partition.flush();
                    driver.getTableFactory().getAdmin().splitTable(tableName);
                }
                newCells.clear();
            }
            srs.close();
        }
        htable.close();
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);
    }

    @Test
    public void multipleSplits() throws Exception {
        spliceClassWatcher.executeUpdate(String.format("insert into %s select col1+" + ITERATIONS+", col2 from %s"
                ,SCHEMA + ".G",SCHEMA + ".A"));
        String tableName = sqlUtil.getConglomID(spliceTableWatcherG.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        partition.flush();
        driver.getTableFactory().getAdmin().splitTable(tableName);
        Thread.sleep(2000);
        driver.getTableFactory().getAdmin().splitTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
            Scan scan = new Scan();
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,partition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                if (i==ITERATIONS/2) {
                    driver.getTableFactory().getAdmin().splitTable(tableName);
                }
                Assert.assertTrue("Empty Cells Should Not Be Returned",newCells!=null&&!newCells.isEmpty());
                newCells.clear();
            }
            srs.close();
        htable.close();
        Assert.assertEquals("Did not return all rows ",2*ITERATIONS,i);
    }

    @Test
    public void testSplitRegionScannerReinit() throws Exception {
        String tableName = sqlUtil.getConglomID(spliceTableWatcherH.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        partition.subPartitions(); // Grabs the latest partitions in cache... / Key to force a split issue
        partition.flush();
        driver.getTableFactory().getAdmin().splitTable(tableName);
        Thread.sleep(2000);
        driver.getTableFactory().getAdmin().splitTable(tableName);
        Thread.sleep(2000);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        int i = 0;
        driver.getTableFactory().getAdmin().splitTable(tableName);
        List<Cell> newCells = new ArrayList<>();
        Scan scan = new Scan();
        SplitRegionScanner srs = new SplitRegionScanner(scan,
                htable,
                clock,partition, driver.getConfiguration(), htable.getConfiguration());
        while (srs.next(newCells)) {
            i++;
            if (i==ITERATIONS/2) {
                driver.getTableFactory().getAdmin().splitTable(tableName);
            }
            Assert.assertTrue("Empty Cells Should Not Be Returned",newCells!=null&&!newCells.isEmpty());
            newCells.clear();
        }
        srs.close();
        htable.close();
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);
    }

    @Test
    public void moveRegionDuringScan() throws SQLException, IOException, InterruptedException {
        String tableNameStr = sqlUtil.getConglomID(spliceTableWatcherI.toString());
        Partition partition = driver.getTableFactory().getTable(tableNameStr);
        Table htable = ((ClientPartition) partition).unwrapDelegate();

        int i = 0;
        try (Admin admin = connection.getAdmin()) {
            HRegionLocation regionLocation = getRegionLocation(tableNameStr, admin);
            Collection<ServerName> allServers = getAllServers(admin);
            ServerName newServer = getNotIn(allServers, regionLocation.getServerName());

            List<Cell> newCells = new ArrayList<>();
            Scan scan = new Scan();
            SplitRegionScanner srs = new SplitRegionScanner(scan, htable, clock, partition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                if (i++ == ITERATIONS / 2) {
                    // JL - TODO Now that we are blocking moves, this hangs.
                   // admin.move(regionLocation.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(newServer.getServerName()));
                }
                newCells.clear();
            }
            srs.close();
            htable.close();
        }
        Assert.assertEquals("Did not return all rows ", ITERATIONS, i);
    }

    @Test
    public void sampledScanWithConcurrentFlushAndSplitTest() throws Exception {

        String tableName = sqlUtil.getConglomID(spliceTableWatcherK.toString());
        Partition partition = driver.getTableFactory()
                .getTable(tableName);
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        List<Partition> partitions = partition.subPartitions();
        int i = 0;
        List<Cell> newCells = new ArrayList<>();
        for (Partition subPartition: partitions){
            Scan scan = new Scan(subPartition.getStartKey(),subPartition.getEndKey());
            scan.setFilter(new SamplingFilter(0.2)); // enable sampling for this scan
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    clock,subPartition, driver.getConfiguration(), htable.getConfiguration());
            while (srs.next(newCells)) {
                i++;
                if (i==(ITERATIONS*0.2)/2) {
                    partition.flush();
                    driver.getTableFactory().getAdmin().splitTable(tableName);
                }
                newCells.clear();
            }
            srs.close();
        }
        htable.close();

        Assert.assertTrue("Returned more rows than expected: " + i, i < (ITERATIONS * 0.35));
        Assert.assertTrue("Returned less rows than expected: " + i, i > (ITERATIONS * 0.05));
    }

}
