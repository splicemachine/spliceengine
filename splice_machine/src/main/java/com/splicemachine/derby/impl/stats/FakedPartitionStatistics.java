package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/23/15
 */
public class FakedPartitionStatistics extends SimpleOverheadManagedPartitionStatistics{

    public FakedPartitionStatistics(String tableId,
                                    String partitionId,
                                    long rowCount,
                                    long totalBytes,
                                    List<ColumnStatistics> columnStatistics){
        super(tableId,
                partitionId,
                rowCount,
                totalBytes,
                columnStatistics);
    }
}
