package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnStatsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.storage.Partition;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A static store for providing PartitionStats entities.
 *
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class PartitionStatsStore {
    private static final Function<? super Partition,? extends String> partitionNameTransform = new Function<Partition, String>(){
        @Override public String apply(Partition hRegionInfo){ return hRegionInfo.getName(); }
    };

    private static final Function<PartitionStatisticsDescriptor,String> partitionStatisticsTransform = new Function<PartitionStatisticsDescriptor, String>(){
        @Override public String apply(PartitionStatisticsDescriptor desc){ return desc.getPartitionId(); }
    };


    public static OverheadManagedTableStatistics getStatistics(long conglomerateId, TransactionController tc) throws StandardException {
        byte[] table = Long.toString(conglomerateId).getBytes();
        List<Partition> partitions = new ArrayList<>();
        getPartitions(table, partitions);
        List<String> partitionNames =Lists.transform(partitions,partitionNameTransform);
        LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
        DataDictionary dd = lcc.getDataDictionary();
        List<PartitionStatisticsDescriptor> partitionStatistics = dd.getPartitionStatistics(conglomerateId, tc);
        Map<String,PartitionStatisticsDescriptor> partitionMap = Maps.uniqueIndex(partitionStatistics,partitionStatisticsTransform);
        List<OverheadManagedPartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        String tableId = Long.toString(conglomerateId);
        PartitionStatisticsDescriptor tStats;
        List<String> missingPartitions = new LinkedList<>();

        for(String partitionName : partitionNames){
            tStats = partitionMap.get(partitionName);
            if(tStats==null) {
                missingPartitions.add(partitionName);
                continue; //skip missing partitions entirely
            }
            List<ColumnStatistics> copy = new ArrayList<>(tStats.getColumnStatsDescriptors().size());
            for (ColumnStatsDescriptor column : tStats.getColumnStatsDescriptors()) {
                copy.add((ColumnStatistics) column.getStats());
            }
            OverheadManagedPartitionStatistics pStats = SimpleOverheadManagedPartitionStatistics.create(tableId,
                    tStats.getPartitionId(),
                    tStats.getRowCount(),
                    tStats.getPartitionSize(),
                    copy);
            partitionStats.add(pStats);
        }

        /*
         * We cannot have *zero* completely populated items unless we have no column statistics, but in that case
         * we have no table information either, so just return an empty list and let the caller figure out
         * what to do
         */
        if (partitionStats.size() <= 0) return emptyStats(tableId,partitions);

        /*
         *
         * 1. Known Partitions which lack statistics (i.e. the region hasn't collected statistics yet)

         * To do this, we first compute an average of data over all *completely populated Partitions*. Then,
         * we take that average and use it to populate cases 1 and 2. Then, we take the averaged column
         * information, and use it to populate any situations in case 3.
         *
\        */
        if(missingPartitions.size()==0)
            return new GlobalStatistics(tableId,partitionStats);
        PartitionAverage average = averageKnown(tableId, partitionStats);

        //fill case 1 scenarios
        for (String missingPartition : missingPartitions) {
            partitionStats.add(average.copy("average-" + missingPartition));
        }

        //we are good, we can return what we go!
        return new GlobalStatistics(Long.toString(conglomerateId), partitionStats);
    }

           /**
         * There are no statistics collected for this table.
         *
         * This is unfortunate, because we still need to *act* like there are statistics, even
         * though we haven't collected anything. To that end, we create a list
         * of "fake" partition statistics--statistics which are based off of pretty much arbitrary
         * values for latency, and which use region size information to build an estimate of
         * rows. This is pretty much ALWAYS not a good estimate, so we make sure and return a different
         * type of statistics, which will allow callers to add warnings etc.
         *
         * Because we base most optimizations off of the latency measures, our arbitrary scaling factors assume
         * that the localreadLatency = 1, and we scale all of our other latencies off of that figure.
         */

    public static GlobalStatistics emptyStats(String tableId,List<Partition> partitions){
        return RegionLoadStatistics.getParameterStatistics(tableId,partitions);
    }

    public static Txn getTxn(TxnView wrapperTxn) throws ExecutionException {
        try {
            return SIDriver.driver().lifecycleManager().beginChildTransaction(wrapperTxn, Txn.IsolationLevel.READ_UNCOMMITTED,null);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private static int getPartitions(byte[] table, List<Partition> partitions) throws StandardException {

        try {
            partitions.addAll(SIDriver.driver().getTableFactory().getTable(table).subPartitions());
            return partitions.size();
        } catch (Exception ioe) {
            throw StandardException.plainWrapException(ioe);
        }
    }

    private static PartitionAverage averageKnown(String tableId,List<OverheadManagedPartitionStatistics> statistics) {
        /*
         * We can make pretty reasonable assumptions about how data is distributed in the GROSS sense--row counts,
         * etc. will tend to be pretty uniformly distributed, because HBase attempts to split regions such
         * that their sizes tend to be roughly equivalent (in that sense, HBase works as an equi-depth histogram
         * on the row keys themselves). Because of this, using average row counts, cardinalities, and so forth
         * will be a reasonable fallback assumption to make.
         *
         * If there are no existing statistics to work from, then we have to fall back still further, but that
         * exercise is left to the caller.
         */
        if(statistics.size()<0) return null;
        PartitionAverage average = new PartitionAverage(tableId,null);
        for(PartitionStatistics partitionStatistics:statistics){
            average.merge(partitionStatistics);
        }
        return average;
    }

}
