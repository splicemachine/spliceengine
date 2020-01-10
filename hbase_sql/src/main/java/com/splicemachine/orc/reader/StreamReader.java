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

import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.stream.StreamSources;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.util.List;

public interface StreamReader {
    ColumnVector readBlock(DataType type)
            throws IOException;

    ColumnVector readBlock(DataType type, ColumnVector columnVector)
            throws IOException;

    void prepareNextRead(int batchSize);

    void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException;

    void startRowGroup(StreamSources dataStreamSources)
            throws IOException;

    int getBatchSize();
}
