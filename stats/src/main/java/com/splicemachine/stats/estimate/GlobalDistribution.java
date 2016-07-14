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

package com.splicemachine.stats.estimate;


import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.TableStatistics;

import java.util.List;

/**
 * A Distribution which is constructed by merging together estimates from all underlying partitions.
 *
 * <p>This class is designed for overriding, by providing protected fields for underlying access. This way,
 * more specific subtypes can be created for e.g. primitive data types.</p>
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public class GlobalDistribution<T extends Comparable<T>> implements Distribution<T> {
    /**
     * A reference to the underlying table statistics used for this distribution.
     *
     * Note: In theory, we could just keep a reference to the partition statistics
     * list directly, and not use the seemingly extra method call for getting the partition
     * statistics here. However, by using the tableStatistics reference instead of an internal
     * field, we are able to allow the tableStatistics entity to rebuild the partitions list
     * under the hood, and therefore we allow the table statistics to be kept up to date (should
     * that be necesssary)
     */
    protected final TableStatistics tableStatistics;
    /**The numerical identifier of the column. This is indexed from 0*/
    protected final int columnId;

    public GlobalDistribution(TableStatistics tableStatistics, int columnId) {
        this.tableStatistics = tableStatistics;
        this.columnId = columnId;
    }

    @Override
    public long selectivity(T element) {
        long selectivityEstimate = 0l;
        List<? extends PartitionStatistics> statistics = tableStatistics.partitionStatistics();
        for(PartitionStatistics partitionStats:statistics){
            Distribution<T> distribution = partitionStats.columnDistribution(columnId);
            selectivityEstimate+=distribution.selectivity(element);
        }
        return selectivityEstimate;
    }

    @Override
    public long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop) {
        long selectivityEstimate = 0l;
        List<? extends PartitionStatistics> statistics = tableStatistics.partitionStatistics();
        for(PartitionStatistics partitionStats:statistics){
            Distribution<T> distribution = partitionStats.columnDistribution(columnId);
            selectivityEstimate+=distribution.rangeSelectivity(start,stop,includeStart,includeStop);
        }
        return selectivityEstimate;
    }

    @Override
    public T minValue(){
        T min = null;
        for(PartitionStatistics pStats:tableStatistics.partitionStatistics()){
            Distribution<T> dist=pStats.columnDistribution(columnId);
            T minValue=dist.minValue();
            if(min==null){
                min =minValue;
            }else if(min.compareTo(minValue)>0)
                min = minValue;
        }
        return min;
    }

    @Override
    public long minCount(){
        T min = null;
        long minCount = 0l;
        for(PartitionStatistics pStats:tableStatistics.partitionStatistics()){
            Distribution<T> distribution=pStats.columnDistribution(columnId);
            T minValue=distribution.minValue();
            if(min==null){
                min =minValue;
                minCount = distribution.minCount();
            }else if(min.compareTo(minValue)>0){
                min=minValue;
                minCount=distribution.minCount();
            }
        }
        return minCount;
    }

    @Override
    public T maxValue(){
        T max = null;
        for(PartitionStatistics pStats:tableStatistics.partitionStatistics()){
            Distribution<T> dist=pStats.columnDistribution(columnId);
            T maxValue=dist.maxValue();
            if(max==null){
                max =maxValue;
            }else if(max.compareTo(maxValue)>0)
                max = maxValue;
        }
        return max;
    }

    @Override
    public long totalCount(){
        long tc = 0l;
        for(PartitionStatistics pStats:tableStatistics.partitionStatistics()){
            tc+=pStats.columnDistribution(columnId).totalCount();
        }
        return tc;
    }

}
