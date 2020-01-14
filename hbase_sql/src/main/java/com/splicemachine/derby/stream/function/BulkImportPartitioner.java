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
import java.util.Random;

/**
 * Created by jyuan on 3/20/17.
 */
public class BulkImportPartitioner extends Partitioner implements
        com.splicemachine.derby.stream.function.Partitioner<Object>, Externalizable {

    private List<BulkImportPartition> partitionList;
    private int tasksPerRegion;
    private boolean initialized;
    private Random random;

    public BulkImportPartitioner() {
    }

    public BulkImportPartitioner(List<BulkImportPartition> bulkImportPartitions,
                                 int tasksPerRegion) {
        this.partitionList = bulkImportPartitions;
        this.tasksPerRegion = tasksPerRegion;
    }

    @Override
    public int getPartition(Object o) {
        if (!initialized) {
            initialize();
            initialized = true;
        }
        Tuple2<Long, byte[]> key =  (Tuple2<Long, byte[]>) o;
        Long conglomerateId = key._1;
        byte[] startKey = key._2;
        BulkImportPartition partitionKey = new BulkImportPartition(conglomerateId, null, startKey, startKey, null);
        int num = Collections.binarySearch(partitionList, partitionKey, BulkImportUtils.getSearchComparator());
        return tasksPerRegion > 1 ? num*tasksPerRegion+random.nextInt(tasksPerRegion) : num;
    }

    @Override
    public int numPartitions() {
        return partitionList.size() * tasksPerRegion;
    }

    @Override
    public void initialize() {
        random = new Random();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionList.size());
        for (int i = 0; i < partitionList.size(); ++i) {
            out.writeObject(partitionList.get(i));
        }
        out.writeInt(tasksPerRegion);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int n = in.readInt();
        partitionList = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            BulkImportPartition partition = (BulkImportPartition) in.readObject();
            partitionList.add(partition);
        }
        tasksPerRegion = in.readInt();
    }
}
