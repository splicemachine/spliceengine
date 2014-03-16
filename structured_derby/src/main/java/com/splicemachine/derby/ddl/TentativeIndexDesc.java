package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.carrotsearch.hppc.BitSet;

public class TentativeIndexDesc implements TentativeDDLDesc, Externalizable {
    long conglomerateNumber;
    long baseConglomerateNumber;
    int[] indexColsToMainColMap;
    boolean unique;
    boolean uniqueWithDuplicateNulls;
    BitSet descColumns;

    /** For serialization, don't use */
    public TentativeIndexDesc() {
        super();
    }

    public TentativeIndexDesc(long conglomerateNumber, long baseConglomerateNumber, int[] indexColsToMainColMap, boolean unique, boolean uniqueWithDuplicateNulls, BitSet descColumns) {
        super();
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.indexColsToMainColMap = indexColsToMainColMap;
        this.unique = unique;
        this.uniqueWithDuplicateNulls = uniqueWithDuplicateNulls;
        this.descColumns = descColumns;
    }

    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    public int[] getIndexColsToMainColMap() {
        return indexColsToMainColMap;
    }

    public boolean isUnique() { return unique; }

    public boolean isUniqueWithDuplicateNulls() {
        return uniqueWithDuplicateNulls;
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
        out.writeBoolean(uniqueWithDuplicateNulls);
        out.writeInt(descColumns.wlen);
        out.writeObject(descColumns.bits);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateNumber = in.readLong();
        baseConglomerateNumber = in.readLong();
        indexColsToMainColMap = (int[]) in.readObject();
        unique = in.readBoolean();
        uniqueWithDuplicateNulls = in.readBoolean();
        int length = in.readInt();
        long[] bits = (long[]) in.readObject();
        descColumns = new BitSet(bits, length);
    }
}
