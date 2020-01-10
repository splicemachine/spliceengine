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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;

public interface LongStream
        extends ValueStream<LongStreamCheckpoint>
{
    long next()
            throws IOException;

    void nextIntVector(int items, int[] vector)
            throws IOException;

    void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException;

    void nextLongVector(int items, long[] vector)
            throws IOException;

    void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException;

    void nextLongVector(DataType type, int items, ColumnVector columnVector)
            throws IOException;

    void nextLongVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException;

    void nextIntVector(DataType type, int items, ColumnVector columnVector)
            throws IOException;

    void nextIntVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException;

    void nextShortVector(int items, short[] vector)
            throws IOException;

    void nextShortVector(int items, short[] vector, boolean[] isNull)
            throws IOException;

    void nextShortVector(DataType type, int items, ColumnVector columnVector)
            throws IOException;

    void nextShortVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException;

    long sum(int items)
            throws IOException;
}
