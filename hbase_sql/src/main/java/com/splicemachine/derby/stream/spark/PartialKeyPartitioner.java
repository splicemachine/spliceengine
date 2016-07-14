/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
