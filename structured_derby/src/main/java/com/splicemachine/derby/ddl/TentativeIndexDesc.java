package com.splicemachine.derby.ddl;

import java.io.Serializable;
import java.util.BitSet;

public class TentativeIndexDesc implements Serializable {
    long conglomerateNumber;
    long baseConglomerateNumber;
    int[] indexColsToMainColMap;
    boolean unique;
    BitSet descColumns;

    public TentativeIndexDesc(long conglomerateNumber, long baseConglomerateNumber, int[] indexColsToMainColMap, boolean unique, BitSet descColumns) {
        super();
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.indexColsToMainColMap = indexColsToMainColMap;
        this.unique = unique;
        this.descColumns = descColumns;
    }

    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    public int[] getIndexColsToMainColMap() {
        return indexColsToMainColMap;
    }

    public boolean isUnique() {
        return unique;
    }

    public BitSet getDescColumns() {
        return descColumns;
    }

    public long getBaseConglomerateNumber() {
        return baseConglomerateNumber;
    }
}
