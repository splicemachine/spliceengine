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

import org.sparkproject.guava.collect.Lists;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.EmptyDistribution;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class PartitionAverage implements OverheadManagedPartitionStatistics {
    private final String tableId;
    private final String partitionId;
    private long totalRowCount;
    private int mergeCount;
    private long totalSize;
    private long totalQueryCount;
    private double totalLocalReadLatency;
    private double totalRemoteReadLatency;
    private long totalLocalReadTime;
    private long totalRemoteReadTime;
    private long totalCollectionTime;

    private double totalOpenScannerLatency;
    private double totalCloseScannerLatency;
    private long numOpenEvents;
    private long numCloseEvents;

    private List<ColumnStatistics> columnStats;

    public PartitionAverage(String tableId, String partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public PartitionAverage copy(String newPartition){
        PartitionAverage pa = new PartitionAverage(tableId,newPartition);
        pa.totalRowCount = totalRowCount;
        pa.mergeCount = mergeCount;
        pa.totalSize = totalSize;
        pa.totalQueryCount = totalQueryCount;
        pa.totalLocalReadLatency = totalLocalReadLatency;
        pa.totalRemoteReadLatency = totalRemoteReadLatency;
        pa.totalLocalReadTime = totalLocalReadTime;
        pa.totalRemoteReadTime = totalRemoteReadTime;
        pa.totalCollectionTime = totalCollectionTime;
        pa.totalOpenScannerLatency = totalOpenScannerLatency;
        pa.totalCloseScannerLatency = totalCloseScannerLatency;
        pa.numOpenEvents = numOpenEvents;
        pa.numCloseEvents = numCloseEvents;
        if (columnStats!=null) {
            pa.columnStats = Lists.newArrayList(columnStats);
        }
        return pa;
    }

    @Override
    public double getOpenScannerLatency(){
        if(numOpenEvents<=0) return 0d;
        return totalOpenScannerLatency/numOpenEvents;
    }

    @Override
    public double getCloseScannerLatency(){
        if(numCloseEvents<=0) return 0d;
        return totalCloseScannerLatency/numCloseEvents;
    }

    @Override public long numOpenEvents(){ return numOpenEvents; }
    @Override public long numCloseEvents(){ return numCloseEvents; }

    @Override
    public long rowCount() {
        if(mergeCount<=0) return 0l;
        return totalRowCount/mergeCount;
    }

    @Override
    public long totalSize() {
        if(mergeCount<=0) return 0l;
        return totalSize/mergeCount;
    }

    @Override
    public int avgRowWidth() {
        if(mergeCount<=0) return 0;
        if(totalRowCount<=0) return 0;
        return (int)(totalSize/totalRowCount);
    }

    @Override
    public long queryCount() {
        if(mergeCount<=0) return 0;
        return totalQueryCount/mergeCount;
    }

    @Override public String partitionId() { return partitionId; }
    @Override public String tableId() { return tableId; }

    @Override
    public double localReadLatency() {
        if(mergeCount<=0) return 0l;
        return totalLocalReadLatency /mergeCount;
    }

    @Override
    public double remoteReadLatency() {
        if(mergeCount<=0) return 0l;
        return totalRemoteReadLatency /mergeCount;
    }

    @Override
    public long localReadTime() {
        if(mergeCount<=0)
            return 0l;
        return totalLocalReadTime/mergeCount;
    }

    @Override
    public long remoteReadTime() {
        if(mergeCount<=0)
            return 0l;
        return totalRemoteReadTime/mergeCount;
    }

    @Override
    public long collectionTime() {
        if(mergeCount<=0) return 0;
        return totalCollectionTime/mergeCount;
    }

    @Override
    public List<ColumnStatistics> columnStatistics() {
        return columnStats;
    }

    @Override
    public <T extends Comparable<T>> Distribution<T> columnDistribution(int columnId) {
        for(ColumnStatistics stats:columnStats){
            if(stats.columnId()==columnId) //noinspection unchecked
                return stats.getDistribution();
        }
        return EmptyDistribution.emptyDistribution();
    }

    @Override
    public <T> ColumnStatistics<T> columnStatistics(int columnId){
    	if (columnStats != null) {
	        for(ColumnStatistics stats:columnStats){
	            if(stats.columnId()==columnId) return stats;
	        }
    	}
        return null;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        totalRowCount+=other.rowCount();
        totalSize+=other.totalSize();
        totalQueryCount+=other.queryCount();
        totalLocalReadLatency+=other.localReadLatency();
        totalRemoteReadLatency+=other.remoteReadLatency();
        totalLocalReadTime+=other.localReadTime();
        totalRemoteReadTime+=other.remoteReadTime();
        totalCollectionTime+=other.collectionTime();
        mergeColumns(other.columnStatistics());
        mergeCount++;

        if(other instanceof OverheadManagedPartitionStatistics){
            OverheadManagedPartitionStatistics ombs = (OverheadManagedPartitionStatistics)other;
            totalOpenScannerLatency+=ombs.getOpenScannerLatency()*ombs.numOpenEvents();
            numOpenEvents+=ombs.numOpenEvents();
            totalCloseScannerLatency+=ombs.getCloseScannerLatency()*ombs.numCloseEvents();
            numCloseEvents+=ombs.numCloseEvents();
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionAverage)) return false;

        PartitionAverage that = (PartitionAverage) o;

        return partitionId.equals(that.partitionId) && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
        int result = tableId.hashCode();
        result = 31 * result + partitionId.hashCode();
        return result;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void mergeColumns(List<ColumnStatistics> columnStatisticses) {
        if(columnStats==null){
            columnStats = new ArrayList<>(columnStatisticses.size());
        }
        for(int i=0;i<columnStatisticses.size();i++){
            ColumnStatistics toMerge = columnStatisticses.get(i);
            boolean found = false;
            for(int j=0;j<columnStats.size();j++){
                ColumnStatistics myStats = columnStats.get(j);
                if(myStats.columnId()==toMerge.columnId()) {
                    myStats.merge(toMerge);
                    //we don't have to reset the list element, because we know myStats implementation is mutable
                    found =true;
                    break;
                }
            }
            if(!found){
                columnStats.add(ColumnAverage.fromExisting(toMerge));
            }
        }
    }
}
