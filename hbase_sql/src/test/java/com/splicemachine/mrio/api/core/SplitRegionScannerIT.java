package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SplitRegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class SplitRegionScannerIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SplitRegionScannerIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SplitRegionScannerIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherC = new SpliceTableWatcher("C",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherD = new SpliceTableWatcher("D",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherE = new SpliceTableWatcher("E",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherF = new SpliceTableWatcher("F",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherG = new SpliceTableWatcher("G",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherH = new SpliceTableWatcher("H",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherI = new SpliceTableWatcher("I",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
    protected static SpliceTableWatcher spliceTableWatcherJ = new SpliceTableWatcher("J",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");

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
					PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SplitRegionScannerIT.class.getSimpleName() + ".A (col1,col2) values (?,?)");
					for (int i = 0; i< ITERATIONS; i++) {
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
                        psA.executeUpdate();
					}
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                    ,SplitRegionScannerIT.class.getSimpleName() + ".B",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                    ,SplitRegionScannerIT.class.getSimpleName() + ".C",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SplitRegionScannerIT.class.getSimpleName() + ".D",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SplitRegionScannerIT.class.getSimpleName() + ".E",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SplitRegionScannerIT.class.getSimpleName() + ".F",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SplitRegionScannerIT.class.getSimpleName() + ".G",SplitRegionScannerIT.class.getSimpleName() + ".A"));
                    spliceClassWatcher.executeUpdate(String.format("insert into %s select * from %s"
                            ,SplitRegionScannerIT.class.getSimpleName() + ".H",SplitRegionScannerIT.class.getSimpleName() + ".A"));





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
                    htable,instance.getConnection(),
                    clock,subPartition);
            while (srs.next(newCells)) {
                i++;
                simpleScan.add(Bytes.toHex(newCells.get(0).getRow()));
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
                    htable,instance.getConnection(),
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
                htable,instance.getConnection(),
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
                ,SplitRegionScannerIT.class.getSimpleName() + ".D",SplitRegionScannerIT.class.getSimpleName() + ".A"));
        spliceClassWatcher.executeUpdate(String.format("insert into %s values (-1,'foo')" // Test A Record before
                ,SplitRegionScannerIT.class.getSimpleName() + ".D"));

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
                    htable,instance.getConnection(),
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
                ,SplitRegionScannerIT.class.getSimpleName() + ".E",SplitRegionScannerIT.class.getSimpleName() + ".A"));
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
                    htable,instance.getConnection(),
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
                ,SplitRegionScannerIT.class.getSimpleName() + ".F",SplitRegionScannerIT.class.getSimpleName() + ".A"));
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
                    htable,instance.getConnection(),
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
    public void multipleSplits() throws Exception {
        System.out.println("Before Write -->");
        spliceClassWatcher.executeUpdate(String.format("insert into %s select col1+" + ITERATIONS+", col2 from %s"
                ,SplitRegionScannerIT.class.getSimpleName() + ".G",SplitRegionScannerIT.class.getSimpleName() + ".A"));
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
                    htable,instance.getConnection(),
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
                htable,instance.getConnection(),
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


}
