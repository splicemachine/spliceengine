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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
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

public class MapStreamReader
        extends AbstractStreamReader {
    private final StreamDescriptor streamDescriptor;

    private final StreamReader keyStreamReader;
    private final StreamReader valueStreamReader;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<LongStream> lengthStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream lengthStream;

    private boolean rowGroupOpen;

    public MapStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.keyStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone);
        this.valueStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(1), hiveStorageTimeZone);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnVector readBlock(DataType type)
            throws IOException {
        return readBlock(type,null); // Cannot Allocate a Map Vector
    }

    @Override
    public ColumnVector readBlock(DataType type, ColumnVector vector)
            throws IOException {
        MapType mapType = (MapType)type;

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
                long entrySkipSize = lengthStream.sum(readOffset);
                keyStreamReader.prepareNextRead(toIntExact(entrySkipSize));
                valueStreamReader.prepareNextRead(toIntExact(entrySkipSize));
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
            } else {
                // All Nulls
                // iterate over structs
            }
        }

        int entryCount = 0;
        for (int length : lengthVector) {
            entryCount += length;
        }
        /*
        //Convert map to array
        if (entryCount > 0) {
            keyStreamReader.prepareNextRead(entryCount);
            valueStreamReader.prepareNextRead(entryCount);
            keys = keyStreamReader.readBlock(keyType);
            values = valueStreamReader.readBlock(valueType);
        }
        else {
            keys = keyType.createBlockBuilder(new BlockBuilderStatus(), 0).build();
            values = valueType.createBlockBuilder(new BlockBuilderStatus(), 1).build();
        }

        InterleavedBlock keyValueBlock = createKeyValueBlock(nextBatchSize, keys, values, lengthVector);

        // convert lengths into offsets into the keyValueBlock (e.g., two positions per entry)
        int[] offsets = new int[nextBatchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int length = lengthVector[i - 1] * 2;
            offsets[i] = offsets[i - 1] + length;
        }
        ArrayBlock arrayBlock = new ArrayBlock(nextBatchSize, nullVector, offsets, keyValueBlock);

        readOffset = 0;
        nextBatchSize = 0;
    */
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

        keyStreamReader.startStripe(dictionaryStreamSources, encoding);
        valueStreamReader.startStripe(dictionaryStreamSources, encoding);
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

        keyStreamReader.startRowGroup(dataStreamSources);
        valueStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
