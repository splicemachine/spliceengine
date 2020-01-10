package com.splicemachine.access.client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
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

/**
 * Created by jyuan on 5/13/19.
 */
public class SpliceCellComparator extends CellComparatorImpl {

    public static final SpliceCellComparator INSTANCE = new SpliceCellComparator();
    @Override
    public int compare(Cell o1, Cell o2, boolean ignoreSequenceid) {
        // Generated Timestamp Check
        if (o1.getTimestamp() == 0l)
            return -1;
        else if (o2.getTimestamp() == 0l)
            return 1;
        else if (o1.getTimestamp() == HConstants.LATEST_TIMESTAMP)
            return 1;
        else if (o2.getTimestamp() == HConstants.LATEST_TIMESTAMP)
            return -1;
        return super.compare(o1,o2, ignoreSequenceid);
    }
}
