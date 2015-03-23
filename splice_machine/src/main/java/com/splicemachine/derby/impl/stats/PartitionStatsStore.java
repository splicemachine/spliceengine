package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.async.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;
import com.splicemachine.stats.TableStatistics;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A store for providing PartitionStats entities.
 *
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class PartitionStatsStore {
    private final RegionCache regionCache;
    private final TableStatisticsStore tableStatsReader;
    private final ColumnStatisticsStore columnStatsReader;
    private static final Function<? super HRegionInfo,? extends String> partitionNameTransform = new Function<HRegionInfo, String>(){
        @Override public String apply(HRegionInfo hRegionInfo){ return hRegionInfo.getEncodedName(); }
    };

    public PartitionStatsStore(RegionCache regionCache,
                               TableStatisticsStore tableStatsReader,
                               ColumnStatisticsStore columnStatsReader) {
        this.regionCache = regionCache;
        this.tableStatsReader = tableStatsReader;
        this.columnStatsReader = columnStatsReader;
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    public TableStatistics getStatistics(TxnView wrapperTxn,long conglomerateId) throws ExecutionException {
        Txn txn = getTxn(wrapperTxn);
        try {
            return fetchTableStatistics(conglomerateId, txn);
        }finally {
            try {
                txn.commit();
            } catch (IOException ignored) {
               //this should never happen, since we are making a read-only transaction
                throw new RuntimeException(ignored);
            }
        }
    }

    public void invalidateCachedStatistics(long conglomerateId) throws ExecutionException{
        List<HRegionInfo> partitions = new ArrayList<>();
        getPartitions(Long.toString(conglomerateId).getBytes(),partitions);

        List<String> partitionNames = Lists.transform(partitions,partitionNameTransform);
        tableStatsReader.invalidate(conglomerateId,partitionNames);
        columnStatsReader.invalidate(conglomerateId,partitionNames);
    }

    /* ****************************************************************************************************************/
    /*private helper methods and classes*/

    private TableStatistics fetchTableStatistics(long conglomerateId, Txn txn) throws ExecutionException {
        byte[] table = Long.toString(conglomerateId).getBytes();

        List<HRegionInfo> partitions = new ArrayList<>();
        int missingPartitions = getPartitions(table, partitions);

        List<String> partitionNames =Lists.transform(partitions,partitionNameTransform);
        TableStatisticsDescriptor[] tableStatses = tableStatsReader.fetchTableStatistics(txn, conglomerateId, partitionNames);
        List<String> noStatsPartitions = new LinkedList<>();
        List<String> columnStatsFetchable = new ArrayList<>();
        for (int i = 0; i < tableStatses.length; i++) {
            TableStatisticsDescriptor tStats = tableStatses[i];
            if (tStats == null) {
                noStatsPartitions.add(partitionNames.get(i));
            } else {
                columnStatsFetchable.add(tStats.getPartitionId());
            }
        }

        Map<String, List<ColumnStatistics>> columnStatsMap = columnStatsReader.fetchColumnStats(txn, conglomerateId, columnStatsFetchable);
        List<PartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        List<TableStatisticsDescriptor> partiallyOccupied = new LinkedList<>();
        String tableId = Long.toString(conglomerateId);
        for(TableStatisticsDescriptor tStats : tableStatses){
            if(tStats==null) continue; //skip missing partitions entirely

            List<ColumnStatistics> columnStats=columnStatsMap.get(tStats.getPartitionId());
            if(columnStats!=null && columnStats.size()>0){
                /*
                 * We have Column Statistics, which means that we have everything we need to build a complete
                 * partition statistics entity here
                 */
                List<ColumnStatistics> copy=new ArrayList<>(columnStats.size());
                for(ColumnStatistics column : columnStats){
                    copy.add(column.getClone());
                }
                PartitionStatistics pStats=new SimplePartitionStatistics(tableId,
                        tStats.getPartitionId(),
                        tStats.getRowCount(),
                        tStats.getPartitionSize(),
                        tStats.getQueryCount(),
                        tStats.getLocalReadLatency(),
                        tStats.getRemoteReadLatency(),
                        copy);
                partitionStats.add(pStats);
            }else if(columnStatsMap.size()>0){
                /*
                     * We have an awkward situation here. We expect that the column statistics are present,
		             * because the table statistics are. However, table statistics may not include column statistics,
		             * because we may not have enabled any. Additionally, column statistics may be absent if we
		             * just recently enabled them but haven't refreshed yet. So in one situation, no partitions
		             * in the table will have columns, but in another situation, some will and some won't. When
		             * Column statistics aren't available for any partition, then we just don't add much. But when
		             * ColumnStatistics *are* available for other partitions, we add this to a list of partitions
		             * to average
		             */
                partiallyOccupied.add(tStats);
            }else{
		            /*
		             * It turns out that there is no available column statistics information for this table. This
		             * occurs when all column collections are disabled, or when the table has just been created. This
		             * is kind of bad news, since we rely on it to generate appropriate distributions. Still,
		             *  we will return what we can
		             */
                PartitionStatistics pStats=new SimplePartitionStatistics(tableId,
                        tStats.getPartitionId(),
                        tStats.getRowCount(),
                        tStats.getPartitionSize(),
                        tStats.getQueryCount(),
                        tStats.getLocalReadLatency(),
                        tStats.getRemoteReadLatency(),
                        Collections.<ColumnStatistics>emptyList());
                partitionStats.add(pStats);
            }
        }

        /*
         * We cannot have *zero* completely populated items unless we have no column statistics, but in that case
         * we have no table information either, so just return an empty list and let the caller figure out
         * what to do
         */
        if (partitionStats.size() <= 0) return emptyStats(tableId,partitions);

        /*
         * We now have separated out our partition map into 4 categories:
         *
         * 1. Completely Missing Partitions (i.e. the RegionCache didn't find a region for a range of bytes)
         * 2. Known Partitions which lack statistics (i.e. the region hasn't collected statistics yet)
         * 3. Known partitions which have only partial data (i.e. table stats, but no column stats)
         * 4. Completely populated Statistics
         *
         * Cases 1 and 2 are the same thing, with only a slight variation (we can explicitly name the partition
         * in case 2, whereas we cannot in case 1)
         *
         * Case 3 is a specialized version of Case 2, in that we have some table statistics, but that's it
         * Case 4 is where we want everything to end up.
         *
         * To do this, we first compute an average of data over all *completely populated Partitions*. Then,
         * we take that average and use it to populate cases 1 and 2. Then, we take the averaged column
         * information, and use it to populate any situations in case 3.
         *
         * Of course, we first want to check if this is necessary--if we have no items in Case 1,2, or
         * 3 states, then we can just return what we have
         */
        if(missingPartitions<=0 && noStatsPartitions.size()<=0 && partiallyOccupied.size()<=0)
            return new GlobalStatistics(tableId,partitionStats);
        PartitionAverage average = averageKnown(tableId, partitionStats);

        //fill case 1 scenarios
        while (missingPartitions > 0) {
            partitionStats.add(average.copy("average-" + missingPartitions));
            missingPartitions--;
        }

        //fill case 2 scenarios
        for (String noStatsPartition : noStatsPartitions) {
            partitionStats.add(average.copy(noStatsPartition));
        }

        //fill case 3 scenario)
        for (TableStatisticsDescriptor tStats: partiallyOccupied) {
            List<ColumnStatistics> avg = average.columnStatistics();
            List<ColumnStatistics> copy = new ArrayList<>(avg.size());
            for (ColumnStatistics avgCol : avg) {
                copy.add(avgCol.getClone());
            }

            PartitionStatistics pStats = new SimplePartitionStatistics(tableId, tStats.getPartitionId(),
                    tStats.getRowCount(),
                    tStats.getPartitionSize(),
                    tStats.getQueryCount(),
                    tStats.getLocalReadLatency(),
                    tStats.getRemoteReadLatency(),
                    copy);
            partitionStats.add(pStats);
        }

        //we are good, we can return what we go!
        return new GlobalStatistics(Long.toString(conglomerateId), partitionStats);
    }

    private GlobalStatistics emptyStats(String tableId,List<HRegionInfo> partitions){
        /*
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
        long perRowLocalLatency = 1;
        long perRowRemoteLatency = 10*perRowLocalLatency; //assume remote reads are 10x more expensive than local

        int numRegions = partitions.size();
        long totalBytes = numRegions*SpliceConstants.regionMaxFileSize; //assume each region is full
        /*
         * We make the rather stupid assumption of assuming that each row occupies 100 bytes (which is a lot,
         * but it should make some things at least relatively reasonable)
         */
        long numRows = totalBytes/100;
        long rowsPerRegion = numRows/partitions.size();

        List<PartitionStatistics> partStats = new ArrayList<>(partitions.size());
        for(HRegionInfo info:partitions){
            partStats.add(new FakedPartitionStatistics(tableId,info.getEncodedName(),
                    numRows,
                    totalBytes,
                    0l,
                    perRowLocalLatency*rowsPerRegion,
                    perRowRemoteLatency*rowsPerRegion,
                    Collections.<ColumnStatistics>emptyList()));
        }
        return new GlobalStatistics(tableId, partStats);
    }

    private Txn getTxn(TxnView wrapperTxn) throws ExecutionException {
        try {
            return TransactionLifecycle.getLifecycleManager().beginChildTransaction(wrapperTxn, Txn.IsolationLevel.READ_UNCOMMITTED,null);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private int getPartitions(byte[] table, List<HRegionInfo> partitions) throws ExecutionException {
        /*
         * Returns the number of partitions which cannot be found from the RegionCache.
         *
         * This will attempt to retry and find any holes which exist in the found region cache,
         * but only for a few times--after that, it will give up and return the number of holes
         * it found. Then the caller can just use the averaging scheme to replace the missing regions
         */
        SortedSet<Pair<HRegionInfo, ServerName>> regions = regionCache.getRegions(table);
        int tryNum = 0;
        int missingSize = regions.size();
        while(missingSize>0 && tryNum<5) {
            byte[] lastEndKey = HConstants.EMPTY_START_ROW;
            List<Pair<byte[],byte[]>> missingRanges = new LinkedList<>();
            for (Pair<HRegionInfo, ServerName> servers : regions) {
                byte[] start = servers.getFirst().getStartKey();
                if (!Bytes.equals(start, lastEndKey)) {
                    /*
                     * We have a gap in the region cache. Let's try to fetch that range again, just
                     * to see if we can get it
                     */
                    missingRanges.add(Pair.newPair(lastEndKey,start));
                }else{
                    partitions.add(servers.getFirst());
                }
                lastEndKey = servers.getFirst().getEndKey();
            }
            missingSize = missingRanges.size();
            regions = new TreeSet<>(new Comparator<Pair<HRegionInfo,ServerName>>() {
                @Override
                public int compare(Pair<HRegionInfo, ServerName> o1, Pair<HRegionInfo, ServerName> o2) {
                    return o1.getFirst().compareTo(o2.getFirst());
                }
            });
            regionCache.invalidate(table);
            for(Pair<byte[],byte[]> range:missingRanges){
                SortedSet<Pair<HRegionInfo, ServerName>> regionsInRange = regionCache.getRegionsInRange(table, range.getFirst(), range.getSecond());
                if(regionsInRange.size()>0) missingSize--;
                regions.addAll(regionsInRange);
            }

            tryNum++;
        }
        return missingSize;
    }

    private PartitionAverage averageKnown(String tableId,List<PartitionStatistics> statistics) {
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
