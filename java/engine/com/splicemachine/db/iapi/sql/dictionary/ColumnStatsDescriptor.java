package com.splicemachine.db.iapi.sql.dictionary;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class ColumnStatsDescriptor  extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private long columnId;
    private Object object;

    public ColumnStatsDescriptor(long conglomerateId,
                                 String partitionId,
                                 long columnId,
                                 Object object) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.columnId = columnId;
        this.object = object;
    }

    public long getColumnId() { return columnId; }
    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public Object getStats() { return object; }
}
