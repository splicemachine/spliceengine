package com.splicemachine.derby.iapi.catalog;

import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class ColumnStatsDescriptor  extends TupleDescriptor{
    private long conglomerateId;
    private String partitionId;
    private long columnId;
    private long nullCount;
    private byte[] cardinality;
    private byte[] frequentElements;
    private byte[] distribution;
    private byte[] minElement;
    private long minFreq;
    private byte[] maxElement;
    private long maxFreq;

    public ColumnStatsDescriptor(long conglomerateId,
                                 String partitionId,
                                 long columnId,
                                 long nullCount,
                                 byte[] cardinality,
                                 byte[] frequentElements,
                                 byte[] distribution,
                                 byte[] minElement,
                                 long minFreq,
                                 byte[] maxElement,
                                 long maxFreq) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.columnId = columnId;
        this.nullCount = nullCount;
        this.cardinality = cardinality;
        this.frequentElements = frequentElements;
        this.distribution = distribution;
        this.minElement = minElement;
        this.minFreq = minFreq;
        this.maxElement = maxElement;
        this.maxFreq = maxFreq;
    }

    public long getColumnId() { return columnId; }
    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public long getNullCount() { return nullCount; }
    public byte[] getCardinality() { return cardinality; }
    public byte[] getFrequentElements() { return frequentElements; }
    public byte[] getDistribution() { return distribution; }
    public byte[] getMinElement() { return minElement; }
    public long getMinFreq() { return minFreq; }
    public byte[] getMaxElement() { return maxElement; }
    public long getMaxFreq() { return maxFreq; }
}
