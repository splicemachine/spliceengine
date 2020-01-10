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
import com.splicemachine.orc.stream.*;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SliceDirectStreamReader
        extends AbstractStreamReader {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final StreamDescriptor streamDescriptor;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;
    private boolean[] isNullVector = new boolean[0];

    @Nonnull
    private StreamSource<LongStream> lengthStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream lengthStream;
    private int[] lengthVector = new int[0];

    @Nonnull
    private StreamSource<ByteArrayStream> dataByteSource = missingStreamSource(ByteArrayStream.class);
    @Nullable
    private ByteArrayStream dataStream;

    private boolean rowGroupOpen;

    public SliceDirectStreamReader(StreamDescriptor streamDescriptor)
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
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but length stream is not present");
                }
                long dataSkipSize = lengthStream.sum(readOffset);
                if (dataSkipSize > 0) {
                    if (dataStream == null) {
                        throw new OrcCorruptionException("Value is not null but data stream is not present");
                    }
                    dataStream.skip(dataSkipSize);
                }
            }
        }

        if (isNullVector.length < nextBatchSize) {
            isNullVector = new boolean[nextBatchSize];
        }
        if (lengthVector.length < nextBatchSize) {
            lengthVector = new int[nextBatchSize];
        }
        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException("Value is not null but length stream is not present");
            }
            Arrays.fill(isNullVector, false);
            lengthStream.nextIntVector(nextBatchSize, lengthVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but length stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, lengthVector, isNullVector);
            }
        }

        int totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                totalLength += lengthVector[i];
            }
        }

        byte[] data = EMPTY_BYTE_ARRAY;
        if (totalLength > 0) {
            if (dataStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            data = dataStream.next(totalLength);
        }

        int offset = 0;
        for (int i = 0, j = 0; i < nextBatchSize; i++) {
            while (vector.isNullAt(i+j)) {
                vector.appendNull();
                j++;
            }
            if (!isNullVector[i]) {
                int length = lengthVector[i];
                vector.appendByteArray(data,offset,length);
                offset += length;
            } else {
                vector.appendNull();
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
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataByteSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        lengthStreamSource = missingStreamSource(LongStream.class);
        dataByteSource = missingStreamSource(ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        lengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);
        dataByteSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
        lengthStream = null;
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
