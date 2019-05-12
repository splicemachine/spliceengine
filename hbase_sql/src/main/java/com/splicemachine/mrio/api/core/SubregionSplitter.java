/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.mrio.api.core;

import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.InputSplit;
import java.util.List;

/**
 * Created by dgomezferro on 3/13/15.
 *
 * Used to compute a list of splits for a given table smaller than regions
 */
public interface SubregionSplitter {
    List<InputSplit> getSubSplits(Table table, List<Partition> splits, byte[] startRow, byte[] stopRow, int requestedSplits, long tableSize);
}
