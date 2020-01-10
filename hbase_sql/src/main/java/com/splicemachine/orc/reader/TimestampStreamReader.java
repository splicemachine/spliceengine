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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TimestampStreamReader
        extends AbstractStreamReader {
    private static final int MILLIS_PER_SECOND = 1000;

    private final StreamDescriptor streamDescriptor;
    private final long baseTimestampInSeconds;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;
    private boolean[] nullVector = new boolean[0];

    @Nonnull
    private StreamSource<LongStream> secondsStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream secondsStream;

    @Nonnull
    private StreamSource<LongStream> nanosStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream nanosStream;

    private long[] secondsVector = new long[0];
    private long[] nanosVector = new long[0];

    private boolean rowGroupOpen;

    public TimestampStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
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
                if (secondsStream == null) {
                    throw new OrcCorruptionException("Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException("Value is not null but nanos stream is not present");
                }

                secondsStream.skip(readOffset);
                nanosStream.skip(readOffset);
            }
        }

        if (secondsVector.length < nextBatchSize) {
            secondsVector = new long[nextBatchSize];
        }
        if (nanosVector.length < nextBatchSize) {
            nanosVector = new long[nextBatchSize];
        }
        if (presentStream == null) {
            if (secondsStream == null) {
                throw new OrcCorruptionException("Value is not null but seconds stream is not present");
            }
            if (nanosStream == null) {
                throw new OrcCorruptionException("Value is not null but nanos stream is not present");
            }

            secondsStream.nextLongVector(nextBatchSize, secondsVector);
            nanosStream.nextLongVector(nextBatchSize, nanosVector);

            // merge seconds and nanos together
            for (int i = 0, j = 0; i < nextBatchSize; i++) {
                while (vector.isNullAt(i+j)) {
                    vector.appendNull();
                    j++;
                }
                vector.appendLong(decodeTimestamp(secondsVector[i], nanosVector[i], baseTimestampInSeconds));
            }
        }
        else {
            if (nullVector.length < nextBatchSize) {
                nullVector = new boolean[nextBatchSize];
            }
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (secondsStream == null) {
                    throw new OrcCorruptionException("Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException("Value is not null but nanos stream is not present");
                }

                secondsStream.nextLongVector(nextBatchSize, secondsVector, nullVector);
                nanosStream.nextLongVector(nextBatchSize, nanosVector, nullVector);

                // merge seconds and nanos together
                for (int i = 0, j=0; i < nextBatchSize; i++) {
                    while (vector.isNullAt(i+j)) {
                        vector.appendNull();
                        j++;
                    }
                    if (nullVector[i]) {
                        vector.appendNull();
                    }
                    else {
                        vector.appendLong(decodeTimestamp(secondsVector[i], nanosVector[i], baseTimestampInSeconds));
                    }
                }
            }
            else {
                for (int i = 0, j=0; i < nextBatchSize; i++) {
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
        secondsStream = secondsStreamSource.openStream();
        nanosStream = nanosStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        secondsStreamSource = missingStreamSource(LongStream.class);
        nanosStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        secondsStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class);
        nanosStreamSource = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    // This comes from the Apache Hive ORC code
    public static long decodeTimestamp(long seconds, long serializedNanos, long baseTimestampInSeconds)
    {
        long millis = (seconds + baseTimestampInSeconds) * MILLIS_PER_SECOND;
        long nanos = parseNanos(serializedNanos);

        // the rounding error exists because java always rounds up when dividing integers
        // -42001/1000 = -42; and -42001 % 1000 = -1 (+ 1000)
        // to get the correct value we need
        // (-42 - 1)*1000 + 999 = -42001
        // (42)*1000 + 1 = 42001
        if (millis < 0 && nanos != 0) {
            millis -= 1000;
        }
        // Truncate nanos to millis and add to mills
        return millis + (nanos / 1_000_000);
    }

    // This comes from the Apache Hive ORC code
    private static int parseNanos(long serialized)
    {
        int zeros = ((int) serialized) & 0b111;
        int result = (int) (serialized >>> 3);
        if (zeros != 0) {
            for (int i = 0; i <= zeros; ++i) {
                result *= 10;
            }
        }
        return result;
    }
}
