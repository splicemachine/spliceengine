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

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.SamplingFilter;
import com.splicemachine.storage.*;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jyuan on 4/24/16.
 */
@Category({SlowTest.class, SerialTest.class})
public class SplitRegionScannerStressIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SplitRegionScannerStressIT.class);

    public static final String CLASS_NAME = SplitRegionScannerStressIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static int nrows = 20000;
    private static int batchSize = 1000;
    protected static SIDriver driver;
    protected static SConfiguration config;
    protected static HBaseConnectionFactory instance;
    protected static Clock clock;

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
            .around(spliceSchemaWatcher);

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn) throws Exception {
        new TableCreator(conn)
                .withCreate("create table a (c1 int, c2 varchar(56), primary key(c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table b (c1 int, c2 varchar(56), primary key(c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table c (c1 int, c2 varchar(56), primary key(c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table d (c1 int, c2 varchar(56), primary key(c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table e (c1 int, c2 varchar(56), primary key(c1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table f (c1 int, c2 varchar(56), primary key(c1))")
                .create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    private static void asyncFlush(Partition partition) {
        new Thread( () -> {
            try {
                Thread.sleep(1000);
                partition.flush();
            } catch (Exception e) {
                LOG.error(e);
            }
        } ).start();
    }

    @Test
    public void testFlush() throws Throwable {
       loadData("A", new Callback() {
           @Override
           public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
               verifyTableCount(partition, i * batchSize);
           }
       });

        loadDataWithNonoverlappingKeys("A", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i%2==0)
                    asyncFlush(partition);
                verifyTableCount(partition, nrows + i * batchSize);
            }
        });

        loadDataWithOverlappingKeys("A", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i % 2 == 0)
                    asyncFlush(partition);
                verifyTableCount(partition, 2 * nrows + i * batchSize);
            }
        });
    }

    @Test
    public void testSampledFlush() throws Throwable {
        loadData("F", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                verifyTableSample(partition, i * batchSize);
            }
        });

        loadDataWithNonoverlappingKeys("F", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i%2==0)
                    asyncFlush(partition);
                verifyTableSample(partition, nrows + i * batchSize);
            }
        });

        loadDataWithOverlappingKeys("F", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i % 2 == 0)
                    asyncFlush(partition);
                verifyTableSample(partition, 2 * nrows + i * batchSize);
            }
        });
    }

    @Test
    public void testCompaction() throws Throwable {
        loadData("B", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                verifyTableCount(partition, i * batchSize);
            }
        });

        loadDataWithNonoverlappingKeys("B", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i % 2 == 1)
                    partition.compact(true);
                else
                    partition.flush();
                verifyTableCount(partition, nrows + i * batchSize);
            }
        });

        loadDataWithOverlappingKeys("B", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                if (i % 2 == 1)
                    partition.compact(true);
                else
                    partition.flush();
                verifyTableCount(partition, 2 * nrows + i * batchSize);
            }
        });
    }

    @Test
    public void testMove() throws Throwable {
        loadData("C", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Throwable {
                verifyTableCount(partition, i * batchSize);
            }
        });

        loadDataWithNonoverlappingKeys("C", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
                splitTable(partitionAdmin, partition, conglomID, true);
                List<Partition> partitions = partition.subPartitions(true);
                movePartition(partitionAdmin, partitions);
            }
        });

        loadDataWithOverlappingKeys("C", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
                splitTable(partitionAdmin, partition, conglomID, true);
                List<Partition> partitions = partition.subPartitions(true);
                movePartition(partitionAdmin, partitions);
            }
        });
    }

    @Test
    public void testSplit() throws Throwable {
        loadData("D", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                verifyTableCount(partition, i * batchSize);
            }
        });

        loadDataWithNonoverlappingKeys("D", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
                splitTable(partitionAdmin, partition, conglomID, false);
                verifyTableCount(partition, nrows + i * batchSize);
            }
        });

        loadDataWithOverlappingKeys("D", new Callback() {
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Exception {
                String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
                splitTable(partitionAdmin, partition, conglomID, false);
                verifyTableCount(partition, 2 * nrows + i * batchSize);
            }
        });
    }

    @Test
    public void test() throws Throwable {

        loadData("E", new Callback() {
            private Random rand = new Random();
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Throwable {
                //action(partitionAdmin, partition, tableName, i, rand);
                verifyTableCount(partition, i * batchSize);
            }
        });

        loadDataWithNonoverlappingKeys("E", new Callback() {
            private Random rand = new Random();
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Throwable {
                action(partitionAdmin, partition, tableName, i, rand);
                verifyTableCount(partition, i * batchSize+nrows);
            }
        });
        loadDataWithOverlappingKeys("E", new Callback() {
            private Random rand = new Random();
            @Override
            public void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Throwable {
                action(partitionAdmin, partition, tableName, i, rand);
                verifyTableCount(partition, i * batchSize+nrows*2);
            }
        });
    }

    private void action(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i, Random rand)throws Throwable {
        //if (i==0)
        //    HBaseTestUtils.setBlockPreFlush(false);
        switch (rand.nextInt(4)) {
            case 0:
                partition.flush();
                break;
            case 1:
                partition.compact(true);
                break;
            case 2:
                String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
                splitTable(partitionAdmin, partition, conglomID, false);
                break;
            case 3:
                List<Partition> partitions = partition.subPartitions(true);
                movePartition(partitionAdmin, partitions);
                break;
        }
    }
    private void splitTable(PartitionAdmin partitionAdmin, Partition partition, String tableName, boolean wait) throws Exception {
        partitionAdmin.splitTable(tableName);
        if (!wait) return;
        List<Partition> partitions = null;
        long waitUnit = 2000;
        long iterations = 10;
        do{
            partitions = partition.subPartitions(true);
            Thread.sleep(waitUnit);
            iterations--;
        } while (partitions.size()==1 && iterations>0);
    }

    private void movePartition(PartitionAdmin partitionAdmin, List<Partition> partitions) throws Exception {
        for (Partition p:partitions) {
            RangedClientPartition rp = (RangedClientPartition) p;
            String partitionName = rp.getRegionInfo().getEncodedName();
            partitionAdmin.move(partitionName, null);
        }
    }

    private void verifyTableCount(Partition partition, int count) throws Exception {
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        int i = 0;
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
        Assert.assertEquals("Did not return all rows ", count, i);
    }

    private void verifyTableSample(Partition partition, int count) throws Exception {
        Table htable = ((ClientPartition) partition).unwrapDelegate();
        int i = 0;
        List<Cell> newCells = new ArrayList<>();
        Scan scan = new Scan();
        scan.setFilter(new SamplingFilter(0.2));
        SplitRegionScanner srs = new SplitRegionScanner(scan,
                htable,
                clock,partition, driver.getConfiguration(), htable.getConfiguration());
        while (srs.next(newCells)) {
            i++;
            newCells.clear();
        }
        srs.close();
        htable.close();

        Assert.assertTrue("Returned more rows than expected: " + i, i < (count * 0.35));
        Assert.assertTrue("Returned less rows than expected: " + i, i > (count * 0.05));
    }

    private void loadData(String tableName, Callback callback) throws Throwable {

        String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
        PartitionAdmin partitionAdmin = driver.getTableFactory().getAdmin();
        Partition partition = driver.getTableFactory().getTable(conglomID);
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        PreparedStatement ps = conn.prepareStatement(String.format("insert into %s values(?, ?)", tableName));
        HBaseTestUtils.setBlockPreFlush(true);
        for (int i = 0; i < nrows/batchSize; ++i) {
            for (int j = 0; j < batchSize; ++j) {
                int v = (i*batchSize+j)*2;
                ps.setInt(1, v);
                ps.setString(2, "varchar " + v);
                ps.addBatch();
            }
            ps.executeBatch();
            callback.call(partitionAdmin, partition, tableName, i+1);
        }

        HBaseTestUtils.setBlockPreFlush(false);
    }


    private void loadDataWithNonoverlappingKeys(String tableName, Callback callback) throws Throwable {
        String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
        PartitionAdmin partitionAdmin = driver.getTableFactory().getAdmin();
        Partition partition = driver.getTableFactory().getTable(conglomID);
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        PreparedStatement ps = conn.prepareStatement(String.format("insert into %s values(?, ?)", tableName));
        for (int i = 0; i < nrows/batchSize; ++i) {
            for (int j = 0; j < batchSize; ++j) {
                int v = nrows*2+i*batchSize+j;
                ps.setInt(1, v);
                ps.setString(2, "varchar " + v);
                ps.addBatch();
            }
            ps.executeBatch();
            callback.call(partitionAdmin, partition, tableName, i + 1);
        }
    }

    private void loadDataWithOverlappingKeys(String tableName, Callback callback) throws Throwable {
        String conglomID = sqlUtil.getConglomID(CLASS_NAME + "." + tableName);
        PartitionAdmin partitionAdmin = driver.getTableFactory().getAdmin();
        Partition partition = driver.getTableFactory().getTable(conglomID);
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        PreparedStatement ps = conn.prepareStatement(String.format("insert into %s values(?, ?)", tableName));
        for (int i = 0; i < nrows/batchSize; ++i) {
            for (int j = 0; j < batchSize; ++j) {
                int v = (i*batchSize+j)*2+1;
                ps.setInt(1, v);
                ps.setString(2, "varchar " + v);
                ps.addBatch();
            }
            ps.executeBatch();
            callback.call(partitionAdmin, partition, tableName, i + 1);
        }
    }
}
interface Callback {
    void call(PartitionAdmin partitionAdmin, Partition partition, String tableName, int i) throws Throwable;
}
