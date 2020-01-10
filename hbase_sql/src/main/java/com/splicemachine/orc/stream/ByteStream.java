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

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.checkpoint.ByteStreamCheckpoint;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.util.Arrays;
import static com.splicemachine.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;
import static com.splicemachine.orc.stream.OrcStreamUtils.readFully;

public class ByteStream
        implements ValueStream<ByteStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[MIN_REPEAT_SIZE + 127];
    private int length;
    private int offset;
    private long lastReadInputCheckpoint;

    public ByteStream(OrcInputStream input)
    {
        this.input = input;
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    // This is based on the Apache Hive ORC code
    private void readNextBlock()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException("Read past end of buffer RLE byte from %s", input);
        }

        offset = 0;

        // if byte high bit is not set, this is a repetition; otherwise it is a literal sequence
        if ((control & 0x80) == 0) {
            length = control + MIN_REPEAT_SIZE;

            // read the repeated value
            int value = input.read();
            if (value == -1) {
                throw new OrcCorruptionException("Reading RLE byte got EOF");
            }

            // fill buffer with the value
            Arrays.fill(buffer, 0, length, (byte) value);
        }
        else {
            // length is 2's complement of byte
            length = 0x100 - control;

            // read the literals into the buffer
            readFully(input, buffer, 0, length);
        }
    }

    @Override
    public Class<ByteStreamCheckpoint> getCheckpointType()
    {
        return ByteStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(ByteStreamCheckpoint checkpoint)
            throws IOException
    {
        // if the checkpoint is within the current buffer, just adjust the pointer
        if (lastReadInputCheckpoint == checkpoint.getInputStreamCheckpoint() && checkpoint.getOffset() <= length) {
            offset = checkpoint.getOffset();
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
            length = 0;
            offset = 0;
            skip(checkpoint.getOffset());
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (offset == length) {
                readNextBlock();
            }
            long consume = Math.min(items, length - offset);
            offset += consume;
            items -= consume;
        }
    }

    public byte next()
        throws IOException
    {
        if (offset == length) {
            readNextBlock();
        }
        return buffer[offset++];
    }

    public void nextVector(DataType type, long items, ColumnVector columnVector)
            throws IOException {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            columnVector.appendByte(next());
        }
    }

    public void nextVector(DataType type, long items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendByte(next());
            }
        }
    }
}
