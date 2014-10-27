package com.splicemachine.stats;

/**
 * A base class for computing Table Statistics.
 *
 * Note that we store a fixed number of regions in this class--it may
 * be more efficient to extend this so as to more dynamically get
 * the proper number of regions.
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public class BaseTableStatistics implements TableStatistics{
    private final long rowCount;
    private final int avgRowSize;
    private final int numRegions;

    private final long oneRowScanCostMicros;

    public BaseTableStatistics(long rowCount, int avgRowSize, int numRegions, long oneRowScanCostMicros) {
        this.rowCount = rowCount;
        this.avgRowSize = avgRowSize;
        this.numRegions = numRegions;
        this.oneRowScanCostMicros = oneRowScanCostMicros;
    }

    @Override public long rowCount() { return rowCount; }
    @Override public int averageRowSize() { return avgRowSize; }
    @Override public int regionCount() { return numRegions; }

    @Override
    public double estimateScanCost(boolean parallel) {
        if(parallel)
            return estimateParallel(rowCount());
        else
            return estimateSequential(rowCount());
    }

    @Override
    public double estimateScanCost(long numRows, boolean parallel) {
        if(parallel)
            return estimateParallel(numRows);
        else
            return estimateSequential(numRows);

    }

    @Override
    public ColumnStatistics getColumnStatistics(int columnNum) {
        return null;
    }

    @Override
    public ColumnStatistics getKeyStatistics() {
        return null;
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private double estimateParallel(long numRows) {

        return 0;
    }

    private double estimateSequential(long numRows) {
        return 0;
    }
}
