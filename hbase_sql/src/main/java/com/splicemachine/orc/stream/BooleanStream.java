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

import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import static com.google.common.base.Preconditions.checkState;

@SuppressWarnings("NarrowingCompoundAssignment")
public class BooleanStream
        implements ValueStream<BooleanStreamCheckpoint>
{
    private static final int HIGH_BIT_MASK = 0b1000_0000;
    private final ByteStream byteStream;
    private byte data;
    private int bitsInData;

    public BooleanStream(OrcInputStream byteStream)
    {
        this.byteStream = new ByteStream(byteStream);
    }

    private void readByte()
            throws IOException
    {
        checkState(bitsInData == 0);
        data = byteStream.next();
        bitsInData = 8;
    }

    public boolean nextBit()
            throws IOException
    {
        // read more data if necessary
        if (bitsInData == 0) {
            readByte();
        }

        // read bit
        boolean result = (data & HIGH_BIT_MASK) != 0;

        // mark bit consumed
        data <<= 1;
        bitsInData--;

        return result;
    }

    @Override
    public Class<BooleanStreamCheckpoint> getCheckpointType()
    {
        return BooleanStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(BooleanStreamCheckpoint checkpoint)
            throws IOException
    {
        byteStream.seekToCheckpoint(checkpoint.getByteStreamCheckpoint());
        bitsInData = 0;
        skip(checkpoint.getOffset());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        if (bitsInData >= items) {
            data <<= items;
            bitsInData -= items;
        }
        else {
            items -= bitsInData;
            bitsInData = 0;

            byteStream.skip(items >>> 3);
            items &= 0b111;

            if (items != 0) {
                readByte();
                data <<= items;
                bitsInData -= items;
            }
        }
    }

    public int countBitsSet(int items)
            throws IOException
    {
        int count = 0;

        // count buffered data
        if (items > bitsInData && bitsInData > 0) {
            count += bitCount(data);
            items -= bitsInData;
            bitsInData = 0;
        }

        // count whole bytes
        while (items > 8) {
            count += bitCount(byteStream.next());
            items -= 8;
        }

        // count remaining bits
        for (int i = 0; i < items; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            if ((data & HIGH_BIT_MASK) != 0) {
                count++;
            }

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }

        return count;
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public void getSetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            vector[i] = (data & HIGH_BIT_MASK) != 0;

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
    }

    /**
     * Sets the vector element to true if the bit is set, skipping the null values.
     */
    public void getSetBits(int batchSize, boolean[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            if (!isNull[i]) {
                // read more data if necessary
                if (bitsInData == 0) {
                    readByte();
                }

                // read bit
                vector[i] = (data & HIGH_BIT_MASK) != 0;

                // mark bit consumed
                data <<= 1;
                bitsInData--;
            }
        }
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public void getSetBits(DataType type, int batchSize, ColumnVector columnVector)
            throws IOException {
        for (int i = 0, j = 0; i < batchSize; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            columnVector.appendBoolean((data & HIGH_BIT_MASK) != 0);

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
    }

    /**
     * Sets the vector element to true if the bit is set, skipping the null values.
     */
    public void getSetBits(DataType type, int batchSize, ColumnVector columnVector, boolean[] isNull)
            throws IOException {
        for (int i = 0, j = 0; i < batchSize; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                // read more data if necessary
                if (bitsInData == 0) {
                    readByte();
                }
                // read bit
                columnVector.appendBoolean((data & HIGH_BIT_MASK) != 0);
                // mark bit consumed
                data <<= 1;
                bitsInData--;
            }
        }
    }

    /**
     * Sets the vector element to true if the bit is not set.
     */
    public int getUnsetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        int count = 0;
        for (int i = 0; i < batchSize; i++) {
            // read more data if necessary
            if (bitsInData == 0) {
                readByte();
            }

            // read bit
            vector[i] = (data & HIGH_BIT_MASK) == 0;
            if (vector[i]) {
                count++;
            }

            // mark bit consumed
            data <<= 1;
            bitsInData--;
        }
        return count;
    }

    private static int bitCount(byte data)
    {
        return Integer.bitCount(data & 0xFF);
    }
}
