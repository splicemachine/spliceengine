package com.splicemachine.derby.impl.stats;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.async.AsyncHbase;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.derby.impl.sql.catalog.SYSCOLUMNSTATISTICSRowFactory;
import com.splicemachine.derby.impl.sql.catalog.SYSPHYSICALSTATISTICSRowFactory;
import com.splicemachine.derby.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class StatisticsStorage {
    //the autoboxing is necessary, despite what the compiler says
    @SuppressWarnings("UnnecessaryBoxing")
    private static final Object lock = new Integer(-1);
//    private static volatile @ThreadSafe PhysicalStatisticsStore physicalStore;
//    private static volatile @ThreadSafe ColumnStatisticsStore columnStatsStore;
//    private static volatile @ThreadSafe TableStatisticsStore tableStatsStore;

    private static volatile @ThreadSafe PartitionStatsStore partitionStore;

    private static final AtomicBoolean runState = new AtomicBoolean(false);

    public static PartitionStatsStore getPartitionStore(){
        if(!runState.get()) throw new IllegalStateException("Cannot get Partition Store, system has not booted yet");

        return partitionStore;
    }

    public static void start(long tableStatsId, long columnStatsConglomId, long physStatsConglomId) {
        if(runState.get()) return; //already started
        synchronized (lock){
            initializeStore(tableStatsId, columnStatsConglomId, physStatsConglomId);
            runState.set(true);
        }
    }

    public static void ensureRunning(DataDictionary dDictionary) throws StandardException {
        if(runState.get()) return; //we have already booted, hooray for us!
        synchronized (lock){
            if(runState.get()) return; //someone beat us to the lock

            SchemaDescriptor systemSchemaDescriptor = dDictionary.getSystemSchemaDescriptor();
            TableDescriptor tableStats= dDictionary.getTableDescriptor(SYSTABLESTATISTICSRowFactory.TABLENAME_STRING, systemSchemaDescriptor, null);
            TableDescriptor colStats= dDictionary.getTableDescriptor(SYSCOLUMNSTATISTICSRowFactory.TABLENAME_STRING, systemSchemaDescriptor, null);
            TableDescriptor physStats= dDictionary.getTableDescriptor(SYSPHYSICALSTATISTICSRowFactory.TABLENAME_STRING, systemSchemaDescriptor, null);

            initializeStore(tableStats.getHeapConglomerateId(),colStats.getHeapConglomerateId(),physStats.getHeapConglomerateId());
            runState.set(true);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static void initializeStore(long tableStatsId, long columnStatsConglomId, long physStatsConglomId) {
        ThreadFactory refreshThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("statistics-refresher-%d")
                .build();
        ScheduledExecutorService refreshExecutor = Executors.newScheduledThreadPool(4, refreshThreadFactory);

        HBaseClient hbaseClient = AsyncHbase.HBASE_CLIENT;
        byte[] physStatTable = Long.toString(physStatsConglomId).getBytes();
        PhysicalStatisticsStore physicalStore = new CachedPhysicalStatsStore(refreshExecutor, hbaseClient, physStatTable);

        byte[] colStatTable = Long.toString(columnStatsConglomId).getBytes();
        ColumnStatisticsStore columnStatsStore = new HBaseColumnStatisticsStore(refreshExecutor,colStatTable,hbaseClient);

        byte[] tableStatTable = Long.toString(tableStatsId).getBytes();
        TableStatisticsStore tableStatsStore = new HBaseTableStatisticsStore(refreshExecutor,tableStatTable,hbaseClient);

        partitionStore = new PartitionStatsStore(HBaseRegionCache.getInstance(),tableStatsStore,columnStatsStore,physicalStore);
    }

}
