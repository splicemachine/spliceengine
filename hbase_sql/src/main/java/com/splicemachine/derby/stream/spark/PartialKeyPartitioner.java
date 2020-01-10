/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
