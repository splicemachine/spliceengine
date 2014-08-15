package com.splicemachine.derby.ddl;

import org.apache.derby.catalog.UUID;

public class DropTableDDLChangeDesc implements TentativeDDLDesc {

    private long conglomerateNumber;
    private UUID tableId;

    /**
     * Provide the conglomerateNumber of the table being dropped.
     */
    public DropTableDDLChangeDesc(long conglomerateNumber, UUID tableId) {
        this.conglomerateNumber = conglomerateNumber;
        this.tableId = tableId;
    }

    @Override
    public long getBaseConglomerateNumber() {
        return 0L;
    }

    @Override
    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    public UUID getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "DropTableDDLChangeDesc{" +
                ", conglomerateNumber=" + conglomerateNumber +
                ", tableId=" + tableId +
                '}';
    }
}
