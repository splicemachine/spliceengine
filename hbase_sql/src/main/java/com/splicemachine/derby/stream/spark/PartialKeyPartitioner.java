package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Partitioner based on partial keys in the PairDataSet
 *
 */
public class PartialKeyPartitioner extends Partitioner implements Externalizable {
    protected int[] keyColumns;
    protected int numPartitions;

    public PartialKeyPartitioner() {

    }

    public PartialKeyPartitioner(int[] keyColumns, int numPartitions) {
        this.keyColumns = keyColumns;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(numPartitions);
        out.writeInt(keyColumns.length);
        for (int key: keyColumns)
            out.writeInt(key);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        numPartitions = in.readInt();
        keyColumns = new int[in.readInt()];
        for (int i =0; i< keyColumns.length;i++)
            keyColumns[i] = in.readInt();
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object o) {
        if (o == null)
            return 0;
        return Utils.nonNegativeMod(((ExecRow)o).hashCode(keyColumns), numPartitions);
    }
}
