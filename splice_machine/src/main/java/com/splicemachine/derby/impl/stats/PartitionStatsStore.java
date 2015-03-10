package com.splicemachine.derby.impl.stats;

import com.splicemachine.async.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.catalog.PhysicalStatsDescriptor;
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
    private final PhysicalStatisticsStore physicalStatsStore;

    public PartitionStatsStore(RegionCache regionCache,
                               TableStatisticsStore tableStatsReader,
                               ColumnStatisticsStore columnStatsReader,
                               PhysicalStatisticsStore physicalStatsStore) {
        this.regionCache = regionCache;
        this.tableStatsReader = tableStatsReader;
        this.columnStatsReader = columnStatsReader;
        this.physicalStatsStore = physicalStatsStore;
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

    /* ****************************************************************************************************************/
    /*private helper methods and classes*/

    private TableStatistics fetchTableStatistics(long conglomerateId, Txn txn) throws ExecutionException {
        byte[] table = Long.toString(conglomerateId).getBytes();

        List<String> partitions = new ArrayList<>();
        Map<String, PhysicalStatsDescriptor> partitionIdToPhysicalLocationMap = new HashMap<>();
        int missingPartitions = getPartitions(table, partitions, partitionIdToPhysicalLocationMap);

        TableStatisticsStore.TableStats[] tableStatses = tableStatsReader.fetchTableStatistics(txn, conglomerateId, partitions);
        List<String> noStatsPartitions = new LinkedList<>();
        List<String> columnStatsFetchable = new ArrayList<>();
        for (int i = 0; i < tableStatses.length; i++) {
            TableStatisticsStore.TableStats tStats = tableStatses[i];
            if (tStats == null) {
                noStatsPartitions.add(partitions.get(i));
            } else {
                columnStatsFetchable.add(tStats.getPartitionId());
            }
        }

        Map<String, List<ColumnStatistics>> columnStatsMap = columnStatsReader.fetchColumnStats(txn, conglomerateId, columnStatsFetchable);
        List<PartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        List<Pair<TableStatisticsStore.TableStats, PhysicalStatsDescriptor>> partiallyOccupied = new LinkedList<>();
        String tableId = Long.toString(conglomerateId);
        for (int i = 0; i < tableStatses.length; i++) {
            TableStatisticsStore.TableStats tStats = tableStatses[i];
            if (tStats == null) continue; //skip missing partitions entirely

            List<ColumnStatistics> columnStats = columnStatsMap.get(tStats.getPartitionId());
            if (columnStats != null && columnStats.size() > 0) {
            /*
             * We have Column Statistics, which means that we have everything we need to build a complete
             * partition statistics entity here
             */
                PhysicalStatsDescriptor physStats = partitionIdToPhysicalLocationMap.get(tStats.getPartitionId());
                List<ColumnStatistics> copy = new ArrayList<>(columnStats.size());
                for (ColumnStatistics column : columnStats) {
                    copy.add(column.getClone());
                }
                PartitionStatistics pStats = new SimplePartitionStatistics(tableId,
                        tStats.getPartitionId(),
                        tStats.getRowCount(),
                        tStats.getPartitionSize(),
                        tStats.getQueryCount(),
                        physStats.getLocalReadLatency(),
                        physStats.getRemoteReadLatency(),
                        copy);
                partitionStats.add(pStats);
            } else if (columnStatsMap.size() > 0) {
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
                partiallyOccupied.add(Pair.newPair(tStats, partitionIdToPhysicalLocationMap.get(tStats.getPartitionId())));
            } else {
            /*
             * It turns out that there is no available column statistics information for this table. This
             * occurs when all column collections are disabled, or when the table has just been created. This
             * is kind of bad news, since we rely on it to generate appropriate distributions. Still,
             *  we will return what we can
             */
                PhysicalStatsDescriptor physStats = partitionIdToPhysicalLocationMap.get(tStats.getPartitionId());
                PartitionStatistics pStats = new SimplePartitionStatistics(tableId,
                        tStats.getPartitionId(),
                        tStats.getRowCount(),
                        tStats.getPartitionSize(),
                        tStats.getQueryCount(),
                        physStats.getLocalReadLatency(),
                        physStats.getRemoteReadLatency(),
                        Collections.<ColumnStatistics>emptyList());
                partitionStats.add(pStats);
            }
        }

        /*
         * We cannot have *zero* completely populated items unless we have no column statistics, but in that case
         * we have no table information either, so just return an empty list and let the caller figure out
         * what to do
         */
        if (partitionStats.size() <= 0) return new GlobalStatistics(tableId, partitionStats);

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
         */
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
        for (Pair<TableStatisticsStore.TableStats, PhysicalStatsDescriptor> partial : partiallyOccupied) {
            List<ColumnStatistics> avg = average.columnStatistics();
            List<ColumnStatistics> copy = new ArrayList<>(avg.size());
            for (ColumnStatistics avgCol : avg) {
                copy.add(avgCol.getClone());
            }

            TableStatisticsStore.TableStats tStats = partial.getFirst();
            PhysicalStatsDescriptor phyStats = partial.getSecond();
            PartitionStatistics pStats = new SimplePartitionStatistics(tableId, tStats.getPartitionId(),
                    tStats.getRowCount(),
                    tStats.getPartitionSize(),
                    tStats.getQueryCount(),
                    phyStats.getLocalReadLatency(),
                    phyStats.getRemoteReadLatency(),
                    copy);
            partitionStats.add(pStats);
        }

        //we are good, we can return what we go!
        return new GlobalStatistics(Long.toString(conglomerateId), partitionStats);
    }

    private Txn getTxn(TxnView wrapperTxn) throws ExecutionException {
        try {
            return TransactionLifecycle.getLifecycleManager().beginChildTransaction(wrapperTxn, Txn.IsolationLevel.READ_UNCOMMITTED,null);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    private int getPartitions(byte[] table,List<String> partitions,
                              Map<String,PhysicalStatsDescriptor> physicalStatsDescriptorMap) throws ExecutionException {
        List<PhysicalStatsDescriptor> allPhysStats = physicalStatsStore.allPhysicalStats();
        /*
         * Returns the number of partitions which cannot be found from the RegionCache.
         *
         * This will attempt to retry and find any holes which exist in the found region cache,
         * but only for a few times--after that, it will give up and return the number of holes
         * it found. Then the caller can just use the averaging scheme to replace the missing regions
         */
        SortedSet<Pair<HRegionInfo, ServerName>> regions = regionCache.getRegions(table);
        int tryNum = 0;
        int missingSize = 0;
        while(tryNum<5) {
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
                    String encodedName = servers.getFirst().getEncodedName();
                    physicalStatsDescriptorMap.put(encodedName,getPhysicalStats(allPhysStats,servers.getSecond().getHostname()));
                    partitions.add(encodedName);
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

    private PhysicalStatsDescriptor getPhysicalStats(List<PhysicalStatsDescriptor> allPhysStats, String hostname) {
        for(PhysicalStatsDescriptor physStats:allPhysStats){
            if(physStats.getHostName().equals(hostname)){
                return physStats;
            }
        }

        /*
         * We are missing the Physical Statistics for this server. That's weird, because we would expect
         * it to be there,but it might not be (if, for example, the server is starting up for the first time
         * and hasn't had a chance to write it yet). In this case, we will make the "homogeneous server" assumption,
         * where we assume that all servers have the same hardware and are configured similarly. In that case, we
         * can just take the first PhysicalStatsDescriptor we have, because that's really what we want anyway.
         *
         * Alas, it may be possible that there are *no* physical stats descriptors at all. This can occur
         * when the cluster boots for the first time. In that scenario, we just pick some arbitrary numbers
         * for the latencies and go with that
         */
        if(allPhysStats.size()>0)
            return allPhysStats.get(0);
        else{
            return new PhysicalStatsDescriptor(hostname,Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().maxMemory(),SpliceConstants.ipcThreads,500,750,800); //TODO -sf- make sure these are reasonable guesses
        }
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
