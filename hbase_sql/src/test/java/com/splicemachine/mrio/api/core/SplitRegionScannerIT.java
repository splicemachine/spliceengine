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
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SplitRegionScanner;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
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
                    clock,subPartition);
            while (srs.next(newCells)) {
                i++;
                simpleScan.add(CellUtils.toHex(newCells.get(0).getRow()));
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
                    clock,subPartition);
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
                clock,partition);
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
                    clock,subPartition);
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
                    clock,subPartition);
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
                    clock,subPartition);
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
                                                            clock,subPartition);
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
        System.out.println("Iterations: "+i);
        Assert.assertEquals("Did not return all rows ",ITERATIONS,i);
    }

    @Test
    public void multipleSplits() throws Exception {
        System.out.println("Before Write -->");
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
                    clock,partition);
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
        System.out.println("Before Write -->");
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
                clock,partition);
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
        try (HBaseAdmin admin = getHBaseAdmin()) {
            HRegionLocation regionLocation = getRegionLocation(tableNameStr, admin);
            Collection<ServerName> allServers = getAllServers(admin);
            ServerName newServer = getNotIn(allServers, regionLocation.getServerName());

            List<Cell> newCells = new ArrayList<>();
            Scan scan = new Scan();
            SplitRegionScanner srs = new SplitRegionScanner(scan, htable, clock, partition);
            while (srs.next(newCells)) {
                if (i++ == ITERATIONS / 2) {
                    System.out.println("Moving region from "+regionLocation.getServerName().getServerName()+" to "+newServer.getServerName());
                    admin.move(regionLocation.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(newServer.getServerName()));
                }
                newCells.clear();
            }
            srs.close();
            htable.close();
        }
        Assert.assertEquals("Did not return all rows ", ITERATIONS, i);
    }

    @Test
    public void moveRegionDuringScanMoveBack() throws SQLException, IOException, InterruptedException {
        // Currently failing with:
        // java.lang.AssertionError: Did not return all rows
        // Expected :20000
        // Actual   :3399

        //
        // Should see something like:
        // 16:57:25,894 (main) TRACE [c.s.s.SplitRegionScanner] - registerRegionScanner SkeletonClienSideregionScanner[scan={"timeRange":[0,9223372036854775807],"batch":-1,"startRow":"","stopRow":"","loadColumnFamiliesOnDemand":null,"totalColumns":1,"cacheBlocks":true,"families":{"V":["ALL"]},"maxResultSize":-1,"maxVersions":2147483647,"caching":-1},region={ENCODED => 501b4581b218c3cbc4709d67eef3a7e5, NAME => 'splice:2416,,1461362232847.501b4581b218c3cbc4709d67eef3a7e5.', STARTKEY => '', ENDKEY => ''},numberOfRows=0
        // .  .  .
        // Moving region from  10.0.1.32,57303,1461354123498 to 10.0.1.32,57242,1461354072040
        // .  .  .
        // Moving region from  10.0.1.32,57242,1461354072040 to 10.0.1.32,57303,1461354123498
        // .  .  .  .  .  .  .  .  .  .  .  .  .  .
        // 16:57:29,949 (main) DEBUG [c.s.s.SplitRegionScanner] - close

        String tableNameStr = sqlUtil.getConglomID(spliceTableWatcherI.toString());
        Partition partition = driver.getTableFactory().getTable(tableNameStr);
        Table htable = ((ClientPartition) partition).unwrapDelegate();

        int i = 0;
        try (HBaseAdmin admin = getHBaseAdmin()) {
            HRegionLocation regionLocation = getRegionLocation(tableNameStr, admin);
            ServerName oldServer = regionLocation.getServerName();
            ServerName newServer = getNotIn(getAllServers(admin), oldServer);

            List<Cell> newCells = new ArrayList<>();
            Scan scan = new Scan();
            SplitRegionScanner srs = new SplitRegionScanner(scan, htable, clock, partition);
            while (srs.next(newCells)) {
                System.out.print(" . ");
                if (i++ == (int)ITERATIONS / 6) {
                    System.out.println("\n"+i+" - Moving region from  "+regionLocation.getServerName().getServerName()+" to "+newServer.getServerName());
                    admin.move(regionLocation.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(newServer.getServerName()));
                    // give it a little time for the metadata to be refreshed
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                }
                if (i == (int)ITERATIONS / 3) {
                    HRegionLocation newRegionLocation = getRegionLocation(tableNameStr, admin);
                    System.out.println("\n"+i+" - Moving region from  "+newRegionLocation.getServerName().getServerName()+" to "+oldServer.getServerName());
                    admin.move(newRegionLocation.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(oldServer.getServerName()));
                }
                newCells.clear();
            }
            System.out.println();
            srs.close();
            htable.close();
        }
        Assert.assertEquals("Did not return all rows ", ITERATIONS, i);
    }

}
