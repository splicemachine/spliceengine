/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.GlobalDistribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class GlobalStatistics implements OverheadManagedTableStatistics {
    private final String tableId;
    private final List<OverheadManagedPartitionStatistics> partitionStatistics;

    private transient List<ColumnStatistics> combinedColumnStatistics;

    public GlobalStatistics(String tableId,List<OverheadManagedPartitionStatistics> partitionStatistics) {
        this.partitionStatistics = partitionStatistics;
        this.tableId = tableId;
    }

    @Override
    public double openScannerLatency(){
        double tot = 0d;
        long numEvents = 0l;
        for(OverheadManagedPartitionStatistics pStats:partitionStatistics){
            tot+=pStats.getOpenScannerLatency();
            numEvents+=pStats.numOpenEvents();
        }
        if(numEvents<=0l) return 0d;
        return tot/numEvents;
    }

    @Override
    public double closeScannerLatency(){
        double tot = 0d;
        long numEvents = 0l;
        for(OverheadManagedPartitionStatistics pStats:partitionStatistics){
            tot+=pStats.getCloseScannerLatency();
            numEvents+=pStats.numCloseEvents();
        }
        if(numEvents<=0l) return 0d;
        return tot/numEvents;
    }

    @Override
    public long numOpenEvents(){
        long opens = 0l;
        for(OverheadManagedPartitionStatistics stats:partitionStatistics){
            opens+=stats.numOpenEvents();
        }
        return opens;
    }

    @Override
    public long numCloseEvents(){
        long opens = 0l;
        for(OverheadManagedPartitionStatistics stats:partitionStatistics){
            opens+=stats.numCloseEvents();
        }
        return opens;
    }

    @Override
    public long rowCount() {
        long rc = 0;
        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.rowCount();
        return rc;
    }

    @Override
    public long totalSize() {
        long rc = 0;
        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.totalSize();
        return rc;
    }

    @Override
    public long avgSize() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.totalSize();
        return rc/partitionStatistics.size();
    }

    @Override
    public double localReadLatency() {
        double rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.localReadLatency();
        return rc /partitionStatistics.size();
    }

    @Override
    public double remoteReadLatency() {
        double rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.remoteReadLatency();
        return rc /partitionStatistics.size();
    }

    @Override
    public long localReadTime() {
        if(partitionStatistics.size()<=0) return 0l;
        long rc = 0l;
        for(PartitionStatistics statistics:partitionStatistics){
           rc+=statistics.localReadTime();
        }
        return rc/partitionStatistics.size();
    }

    @Override
    public long remoteReadTime() {
        if(partitionStatistics.size()<=0) return 0l;
        long rc = 0l;
        for(PartitionStatistics statistics:partitionStatistics){
            rc+=statistics.remoteReadTime();
        }
        return rc/partitionStatistics.size();
    }

    @Override
    public int avgRowWidth() {
        int rc = 0;
        if(partitionStatistics.size()<=0) return 0;

        for(PartitionStatistics statistics: partitionStatistics) {
            int avg = statistics.avgRowWidth();
            assert avg >= 0 : "avgRowWidth cannot be negative -> totalSize=" + statistics.totalSize() +
                    " rowCount=" + statistics.rowCount() + " className=" + statistics.getClass().getName();
            rc += avg;
        }
        return rc/partitionStatistics.size();
    }

    @Override
    public long queryCount() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.queryCount();
        return rc/partitionStatistics.size();
    }

    @Override public String partitionId() { return tableId(); }
    @Override public String tableId() { return tableId; }

    @Override
    public long collectionTime() {
        long maxColTime = 0l;
        for(PartitionStatistics stat:this.partitionStatistics){
            if(maxColTime<stat.collectionTime())
                maxColTime = stat.collectionTime();
        }
        return maxColTime;
    }

    @Override
    public List<ColumnStatistics> columnStatistics() {
        if(combinedColumnStatistics==null){
            combinedColumnStatistics = mergeColumnStats(partitionStatistics);
        }
        return combinedColumnStatistics;
    }

    @Override
    public String toString() {
        return String.format("GlobalStatistics(%s) {openScannerLatency=%f, closeScannerLatency=%f, numOpenEvents=%d, numCloseEvents=%d," +
                "rowCounts=%d, totalSize=%d, avgSize=%d, localReadLatency=%f, remoteReadLatency=%f, localReadTime=%d, " +
                "remoteReadTime=%d, avgRowWidth=%d, queryCount=%d}",tableId,openScannerLatency(),closeScannerLatency(),
                numOpenEvents(),numCloseEvents(),rowCount(),totalSize(),avgSize(),localReadLatency(),remoteReadLatency(),
                localReadTime(),remoteReadTime(),avgRowWidth(),queryCount());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ColumnStatistics<T> columnStatistics(int columnId){
        List<ColumnStatistics> colStats = columnStatistics();
        for(ColumnStatistics cStats:colStats){
            if(cStats.columnId()==columnId) return (ColumnStatistics<T>)cStats;
        }
        return null;
    }

    @Override
    public <T extends Comparable<T>> Distribution<T> columnDistribution(int columnId) {
        return new GlobalDistribution<>(this,columnId);
    }

    @Override
    public List<? extends PartitionStatistics> partitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        throw new UnsupportedOperationException("Cannot merge Global statistics!");
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private List<ColumnStatistics> mergeColumnStats(List<OverheadManagedPartitionStatistics> partitionStatistics){
        if(partitionStatistics.size()<=0) return Collections.emptyList();
        List<ColumnStatistics> stats = null;
        for(OverheadManagedPartitionStatistics partStats:partitionStatistics){
            List<ColumnStatistics> colStats=partStats.columnStatistics();
            if(colStats==null || colStats.size()<=0) continue;
            if(stats==null)
                stats = new ArrayList<>(colStats.size());
            for(ColumnStatistics cStats:colStats){
                boolean found = false;
                for(ColumnStatistics existing:stats){
                    if(existing.columnId()==cStats.columnId()){
                        //noinspection unchecked
                        existing.merge(cStats);
                        found=true;
                        break;
                    }
                }
                if(!found){
                    ColumnStatistics cAvg = cStats.getClone();
                    stats.add(cAvg);
                }
            }
        }
        if(stats==null) return Collections.emptyList();
        return stats;
    }
}
