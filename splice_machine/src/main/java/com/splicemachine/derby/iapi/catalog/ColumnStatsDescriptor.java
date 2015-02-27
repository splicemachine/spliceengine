package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.stats.frequency.FrequentElements;
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
    private long cardinality;
    private FrequentElements frequentElements;
    private byte[] distribution;
    private Object minElement;
    private long minFreq;
    private Object maxElement;
    private long maxFreq;

    public ColumnStatsDescriptor(long conglomerateId,
                                 String partitionId,
                                 long columnId,
                                 long nullCount,
                                 long cardinality,
                                 FrequentElements frequentElements,
                                 byte[] distribution,
                                 Object minElement,
                                 long minFreq,
                                 Object maxElement,
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
    public long getCardinality() { return cardinality; }
    public FrequentElements getFrequentElements() { return frequentElements; }
    public byte[] getDistribution() { return distribution; }
    public Object getMinElement() { return minElement; }
    public long getMinFreq() { return minFreq; }
    public Object getMaxElement() { return maxElement; }
    public long getMaxFreq() { return maxFreq; }
}
