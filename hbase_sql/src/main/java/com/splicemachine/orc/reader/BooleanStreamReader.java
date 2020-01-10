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

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.StreamDescriptor;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.stream.BooleanStream;
import com.splicemachine.orc.stream.StreamSource;
import com.splicemachine.orc.stream.StreamSources;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import static java.util.Objects.requireNonNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;
import static com.splicemachine.orc.metadata.Stream.StreamKind.PRESENT;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;

public class BooleanStreamReader
        extends AbstractStreamReader {
    private final StreamDescriptor streamDescriptor;


    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;
    private boolean[] nullVector = new boolean[0];

    @Nonnull
    private StreamSource<BooleanStream> dataStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream dataStream;

    private boolean rowGroupOpen;

    public BooleanStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnVector readBlock(DataType type, ColumnVector vector)
            throws IOException
    {

    if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                dataStream.skip(readOffset);
            }
        }
        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            dataStream.getSetBits(type, nextBatchSize, vector);
        }
        else {
            if (nullVector.length < nextBatchSize) {
                nullVector = new boolean[nextBatchSize];
            }
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                dataStream.getSetBits(type, nextBatchSize, vector, nullVector);
            }
            else {
                // need to check nesting!!
                for (int i = 0, j = 0; i < nextBatchSize; i++) {
                    while (vector.isNullAt(i+j)) {
                        vector.appendNull();
                        j++;
                    }
                    vector.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return vector;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        dataStreamSource = missingStreamSource(BooleanStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, BooleanStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
