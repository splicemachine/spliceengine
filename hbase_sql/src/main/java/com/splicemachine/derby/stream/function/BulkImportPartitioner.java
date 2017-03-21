/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function;

import com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import scala.Tuple2;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 3/20/17.
 */
public class BulkImportPartitioner extends Partitioner implements
        com.splicemachine.derby.stream.function.Partitioner<Object>, Externalizable {


    private List<BulkImportPartition> partitionList;

    public BulkImportPartitioner() {
    }

    public BulkImportPartitioner(List<BulkImportPartition> bulkImportPartitions) {
        this.partitionList = bulkImportPartitions;
    }

    @Override
    public int getPartition(Object o) {

        Tuple2<Long, byte[]> key =  (Tuple2<Long, byte[]>) o;
        Long conglomerateId = key._1;
        byte[] startKey = key._2;
        BulkImportPartition partitionKey = new BulkImportPartition(conglomerateId, startKey, startKey, null);
        int num = Collections.binarySearch(partitionList, partitionKey, BulkImportUtils.getSearchComparator());
        return num;
    }

    @Override
    public int numPartitions() {
        return partitionList.size();
    }

    @Override
    public void initialize() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionList.size());
        for (int i = 0; i < partitionList.size(); ++i) {
            out.writeObject(partitionList.get(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int n = in.readInt();
        partitionList = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            BulkImportPartition partition = (BulkImportPartition) in.readObject();
            partitionList.add(partition);
        }
    }
}
