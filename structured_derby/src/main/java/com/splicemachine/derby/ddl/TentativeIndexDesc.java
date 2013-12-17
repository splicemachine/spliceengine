package com.splicemachine.derby.ddl;

import com.carrotsearch.hppc.BitSet;

import java.io.*;

public class TentativeIndexDesc implements Externalizable {
    long conglomerateNumber;
    long baseConglomerateNumber;
    int[] indexColsToMainColMap;
    boolean unique;
    BitSet descColumns;

    /** For serialization, don't use */
    public TentativeIndexDesc() {
    }

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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateNumber);
        out.writeLong(baseConglomerateNumber);
        out.writeObject(indexColsToMainColMap);
        out.writeBoolean(unique);
        out.writeInt(descColumns.wlen);
        out.writeObject(descColumns.bits);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateNumber = in.readLong();
        baseConglomerateNumber = in.readLong();
        indexColsToMainColMap = (int[]) in.readObject();
        unique = in.readBoolean();
        int length = in.readInt();
        long[] bits = (long[]) in.readObject();
        descColumns = new BitSet(bits, length);
    }
}
