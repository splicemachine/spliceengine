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
import com.splicemachine.orc.stream.LongStream;
import com.splicemachine.orc.stream.StreamSource;
import com.splicemachine.orc.stream.StreamSources;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTimeZone;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.splicemachine.orc.metadata.Stream.StreamKind.LENGTH;
import static com.splicemachine.orc.metadata.Stream.StreamKind.PRESENT;
import static com.splicemachine.orc.reader.StreamReaders.createStreamReader;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListStreamReader
        extends AbstractStreamReader {
    private final StreamDescriptor streamDescriptor;

    private final StreamReader elementStreamReader;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<LongStream> lengthStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream lengthStream;

    private boolean rowGroupOpen;

    public ListStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.elementStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone);
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
        ArrayType arrayType = (ArrayType) type;
        DataType elementType = arrayType.elementType();

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
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                long elementSkipSize = lengthStream.sum(readOffset);
                elementStreamReader.prepareNextRead(toIntExact(elementSkipSize));
            }
        }

        // The length vector could be reused, but this simplifies the code below by
        // taking advantage of null entries being initialized to zero.  The vector
        // could be reinitialized for each loop, but that is likely just as expensive
        // as allocating a new array
        int[] lengthVector = new int[nextBatchSize];
        boolean[] nullVector = new boolean[nextBatchSize];
        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, lengthVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, lengthVector, nullVector);
            }
        }
        int[] offsets = new int[nextBatchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int length = lengthVector[i - 1];
            offsets[i] = offsets[i - 1] + length;
        }
        // Put Array Offsets and Lengths
        for (int i =0, j= 0; i< nextBatchSize; i++) {
            while (vector.isNullAt(i+j)) {
                vector.appendNull();
                j++;
            }
            if (nullVector[i+j]) {
                vector.appendNull();
            } else {
                vector.appendNotNull(); // Optimize
                vector.putArray(i+j, offsets[i], lengthVector[i]);
            }
        }

        int elementCount = offsets[offsets.length - 1];
        ColumnVector arrayVector = vector.arrayData();
        if (elementCount > 0) {
            elementStreamReader.prepareNextRead(elementCount);
            arrayVector.reserve(elementCount);
            arrayVector = elementStreamReader.readBlock(elementType,arrayVector);
        }
        else {
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

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        lengthStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        lengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
