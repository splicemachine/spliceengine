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
package com.splicemachine.orc.reader;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;

/**
 *
 */
public abstract class AbstractStreamReader implements StreamReader {
    protected int nextBatchSize;
    protected int readOffset;
    @Override
    public ColumnVector readBlock(DataType type)
            throws IOException {
        return readBlock(type,ColumnVector.allocate(nextBatchSize,type, MemoryMode.ON_HEAP));
    }

    @Override
    public int getBatchSize() {
        return nextBatchSize;
    }
}
