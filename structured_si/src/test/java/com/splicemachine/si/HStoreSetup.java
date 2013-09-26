package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.coprocessors.SIObserverUnPacked;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HHasher;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.Hasher;
import com.splicemachine.si.impl.STableReaderDelegate;
import com.splicemachine.si.impl.SystemClock;
import com.splicemachine.si.impl.Tracer;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class HStoreSetup implements StoreSetup {
    static int nextBasePort = 12000;

    static int getNextBasePort() {
        synchronized (HStoreSetup.class) {
            nextBasePort = nextBasePort + 4 + new Random().nextInt(10) * 100;
            return nextBasePort;
        }
    }

    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;
    final Hasher hasher = new HHasher();
    Clock clock = new SystemClock();

    HBaseTestingUtility testCluster;

    private static boolean useSingleton;
    private static HStoreSetup singleton;

    public synchronized static void setUseSingleton(boolean useSingletonValue) {
        useSingleton = useSingletonValue;
    }

    public synchronized static HStoreSetup create() {
        if (useSingleton) {
            if (singleton == null) {
                singleton = new HStoreSetup(false);
            }
            return singleton;
        } else {
            return new HStoreSetup(false);
        }
    }

    public synchronized static void destroy(HStoreSetup setup) throws Exception {
        if (!useSingleton) {
            setup.getTestCluster().shutdownMiniCluster();
        }
    }

    public HStoreSetup(boolean usePacked) {
        setupHBaseHarness(usePacked);
    }

    public static Map<String, HRegion> regionMap = new HashMap<String, HRegion>();

    private TestHTableSource setupHTableSource(int basePort, boolean usePacked) {
        try {
            testCluster = new HBaseTestingUtility();
            Tracer.registerRegion(new Function<Object[], Object>() {
                @Override
                public Object apply(@Nullable Object[] input) {
                    regionMap.put((String) input[0], (HRegion) input[1]);
                    return null;
                }
            });
            testCluster.getConfiguration().setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
                    usePacked ? SIObserver.class.getName() : SIObserverUnPacked.class.getName());
            setTestingUtilityPorts(testCluster, basePort);

            testCluster.startMiniCluster(1);
            final TestHTableSource tableSource = new TestHTableSource(testCluster, getPersonTableName(),
                    new String[]{SpliceConstants.DEFAULT_FAMILY, SIConstants.SNAPSHOT_ISOLATION_FAMILY});
            tableSource.addTable(testCluster, SpliceConstants.TRANSACTION_TABLE, new String[]{"siFamily", "siChildrenFamily"});
            return tableSource;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setTestingUtilityPorts(HBaseTestingUtility testCluster, int basePort) {
        testCluster.getConfiguration().setInt("hbase.master.port", basePort + 0);
        testCluster.getConfiguration().setInt("hbase.master.info.port", basePort + 1);
        testCluster.getConfiguration().setInt("hbase.regionserver.port", basePort + 2);
        testCluster.getConfiguration().setInt("hbase.regionserver.info.port", basePort + 3);
    }

    private void setupHBaseHarness(boolean usePacked) {
        dataLib = new HDataLib();
        final STableReader<IHTable, Result, Get, Scan, KeyValue, RegionScanner, byte[]> rawReader = new HTableReader(setupHTableSource(getNextBasePort(), usePacked));
        reader = new STableReaderDelegate<IHTable, Result, Get, Scan, KeyValue, RegionScanner, byte[]>(rawReader) {
            @Override
            public void close(IHTable table) {
                // Ignore close calls
            }
        };
        writer = new HTableWriter();
    }

    @Override
    public SDataLib getDataLib() {
        return dataLib;
    }

    @Override
    public STableReader getReader() {
        return reader;
    }

    @Override
    public STableWriter getWriter() {
        return writer;
    }

    @Override
    public Hasher getHasher() {
        return hasher;
    }

    @Override
    public HBaseTestingUtility getTestCluster() {
        return testCluster;
    }

    @Override
    public Object getStore() {
        return null;
    }

    @Override
    public String getPersonTableName() {
        return "999";
    }

    @Override
    public Clock getClock() {
        return clock;
    }
}
