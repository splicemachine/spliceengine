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

/**
 * This interface represent a Partitioner for a datatype T (ExecRow) <br>
 * <br>
 * It partitions a {@link com.splicemachine.derby.stream.iapi.PairDataSet PairDataSet< T, V >} into {@link #numPartitions()}
 * partitions and assigns each T a partition based on {@link #getPartition(Object)} <br>
 * <br>
 * Created by dgomezferro on 2/29/16.
 *
 */
public interface Partitioner<T> {

    void initialize();

    /**
     * @return number of partitions the dataset is partitioned into
     */
    int numPartitions();

    /**
     * Computes the partition a given key is assigned to
     * @param t
     * @return
     */
    int getPartition(T t);
}
