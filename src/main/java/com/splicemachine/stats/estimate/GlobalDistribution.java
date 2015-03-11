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
public class GlobalDistribution<T> implements Distribution<T> {
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
        List<PartitionStatistics> statistics = tableStatistics.partitionStatistics();
        for(PartitionStatistics partitionStats:statistics){
            Distribution<T> distribution = partitionStats.columnDistribution(columnId);
            selectivityEstimate+=distribution.selectivity(element);
        }
        return selectivityEstimate;
    }

    @Override
    public long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop) {
        long selectivityEstimate = 0l;
        List<PartitionStatistics> statistics = tableStatistics.partitionStatistics();
        for(PartitionStatistics partitionStats:statistics){
            Distribution<T> distribution = partitionStats.columnDistribution(columnId);
            selectivityEstimate+=distribution.rangeSelectivity(start,stop,includeStart,includeStop);
        }
        return selectivityEstimate;
    }
}
