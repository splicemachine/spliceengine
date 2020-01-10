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
import org.apache.spark.sql.types.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.math.BigInteger.TEN;
import static java.util.Objects.requireNonNull;

public class DecimalStreamReader
        extends AbstractStreamReader
{
    private static final int LONG_POWERS_OF_TEN_TABLE_LENGTH = 19;
    private static final int BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH = 100;
    private static final long[] LONG_POWERS_OF_TEN = new long[LONG_POWERS_OF_TEN_TABLE_LENGTH];
    private static final BigInteger[] BIG_INTEGER_POWERS_OF_TEN = new BigInteger[BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH];

    static {
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
        }

        for (int i = 0; i < BIG_INTEGER_POWERS_OF_TEN.length; ++i) {
            BIG_INTEGER_POWERS_OF_TEN[i] = TEN.pow(i);
        }
    }

    private final StreamDescriptor streamDescriptor;

    private boolean[] nullVector = new boolean[0];
    private long[] shortDecimalVector = new long[0];
    private BigInteger[] longDecimalVector = new BigInteger[0];
    private long[] scaleVector = new long[0];

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<DecimalStream> decimalStreamSource = missingStreamSource(DecimalStream.class);
    @Nullable
    private DecimalStream decimalStream;

    @Nonnull
    private StreamSource<LongStream> scaleStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream scaleStream;

    private boolean rowGroupOpen;

    public DecimalStreamReader(StreamDescriptor streamDescriptor)
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
        DecimalType decType = (DecimalType) type;
        if (!rowGroupOpen) {
            openRowGroup();
        }

        seekToOffset();
        allocateVectors();
        readStreamsData(decType);
        buildDecimalsBlock(decType, vector);

        readOffset = 0;
        nextBatchSize = 0;

        return vector;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        decimalStream = decimalStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
        rowGroupOpen = true;
    }

    private void seekToOffset()
            throws IOException
    {
        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (decimalStream == null) {
                    throw new OrcCorruptionException("Value is not null but decimal stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException("Value is not null but scale stream is not present");
                }

                decimalStream.skip(readOffset);
                scaleStream.skip(readOffset);
            }
        }
    }

    private void allocateVectors()
    {
        if (nullVector.length < nextBatchSize) {
            nullVector = new boolean[nextBatchSize];
            shortDecimalVector = new long[nextBatchSize];
            longDecimalVector = new BigInteger[nextBatchSize];
            scaleVector = new long[nextBatchSize];
        }
    }

    private void readStreamsData(DecimalType decimalType)
            throws IOException
    {
        if (presentStream == null) {
            if (decimalStream == null) {
                throw new OrcCorruptionException("Value is not null but decimal stream is not present");
            }
            if (scaleStream == null) {
                throw new OrcCorruptionException("Value is not null but scale stream is not present");
            }

            Arrays.fill(nullVector, false);
            if (inLong(decimalType)) {
                decimalStream.nextLongVector(nextBatchSize, shortDecimalVector);
            }
            else {
                decimalStream.nextBigIntegerVector(nextBatchSize, longDecimalVector);
            }

            scaleStream.nextLongVector(nextBatchSize, scaleVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (decimalStream == null) {
                    throw new OrcCorruptionException("Value is not null but decimal stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException("Value is not null but scale stream is not present");
                }

                if (inLong(decimalType)) {
                    decimalStream.nextLongVector(nextBatchSize, shortDecimalVector, nullVector);
                }
                else {
                    decimalStream.nextBigIntegerVector(nextBatchSize, longDecimalVector, nullVector);
                }

                scaleStream.nextLongVector(nextBatchSize, scaleVector, nullVector);
            }
        }
    }

    private ColumnVector buildDecimalsBlock(DecimalType decimalType, ColumnVector vector)
            throws OrcCorruptionException {

        if (inInt(decimalType)) {
            for (int i = 0,j=0; i < nextBatchSize; i++) {
                while (vector.isNullAt(i+j)) {
                    vector.appendNull();
                    j++;
                }
                if (!nullVector[i]) {
                    long rescaledDecimal = rescale(shortDecimalVector[i], (int) scaleVector[i], decimalType.scale());
                    vector.appendInt((int)rescaledDecimal);
                }
                else {
                    vector.appendNull();
                }
            }
        }
        else if (inLong(decimalType)) {
            for (int i = 0, j=0; i < nextBatchSize; i++) {
                while (vector.isNullAt(i+j)) {
                    vector.appendNull();
                    j++;
                }
                if (!nullVector[i]) {
                    long rescaledDecimal = rescale(shortDecimalVector[i], (int) scaleVector[i], decimalType.scale());
                    vector.appendLong(rescaledDecimal);
                }
                else {
                    vector.appendNull();
                }
            }
        }

        else {
            MathContext mc = new MathContext(decimalType.precision());
            for (int i = 0,j=0; i < nextBatchSize; i++) {
                while (vector.isNullAt(i+j)) {
                    vector.appendNull();
                    j++;
                }
                if (!nullVector[i]) {
                    BigDecimal bigDecimal = new BigDecimal(longDecimalVector[i],(int) scaleVector[i],mc);
                    Decimal dec = Decimal.apply(bigDecimal,decimalType.precision(),decimalType.scale());
                    vector.putDecimal(i+j,dec,decimalType.precision());
                    vector.appendNotNull();
                }
                else {
                    vector.appendNull();
                }
            }
        }

        return vector;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        decimalStreamSource = missingStreamSource(DecimalStream.class);
        scaleStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        decimalStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        decimalStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, DecimalStream.class);
        scaleStreamSource = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        decimalStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    public static boolean inInt(DecimalType type) {
        return type.precision() <= Decimal.MAX_INT_DIGITS();
    }

    public static boolean inLong(DecimalType type) {
        return type.precision() <= Decimal.MAX_LONG_DIGITS();
    }


    public static long rescale(long value, int fromScale, int toScale) {
        if (toScale < fromScale) {
            throw new IllegalArgumentException("target scale must be larger than source scale");
        }
        return value * longTenToNth(toScale - fromScale);
    }

    public static long longTenToNth(int n)
    {
        return LONG_POWERS_OF_TEN[n];
    }

}
