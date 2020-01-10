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

import com.splicemachine.orc.StreamDescriptor;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.splicemachine.orc.stream.StreamSources;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import java.io.IOException;
import java.util.List;
import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.*;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SliceStreamReader
        extends AbstractStreamReader {
    private final StreamDescriptor streamDescriptor;
    private final SliceDirectStreamReader directReader;
    private final SliceDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public SliceStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        directReader = new SliceDirectStreamReader(streamDescriptor);
        dictionaryReader = new SliceDictionaryStreamReader(streamDescriptor);
    }

    @Override
    public ColumnVector readBlock(DataType type)
            throws IOException {
        return readBlock(type,ColumnVector.allocate(currentReader.getBatchSize(),type, MemoryMode.ON_HEAP));
    }


    @Override
    public ColumnVector readBlock(DataType type, ColumnVector vector)
            throws IOException
    {
        return currentReader.readBlock(type, vector);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        currentReader.prepareNextRead(batchSize);
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind columnEncodingKind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (columnEncodingKind == DIRECT || columnEncodingKind == DIRECT_V2 || columnEncodingKind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (columnEncodingKind == DICTIONARY || columnEncodingKind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + columnEncodingKind);
        }

        currentReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
