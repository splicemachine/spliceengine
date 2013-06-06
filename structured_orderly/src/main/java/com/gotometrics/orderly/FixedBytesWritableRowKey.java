/*  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.gotometrics.orderly;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * Serializes and deserializes BytesWritable into a fixed length sortable representation.
 * <p/>
 * TODO: this doesn't support NULL values (because they can not be distinguished from empty arrays). Should I
 * explicitely check for this?
 */
public class FixedBytesWritableRowKey extends RowKey {
    private int length;

    public FixedBytesWritableRowKey(int length) {
        this.length = length;
    }

    @Override
    public Class<?> getSerializedClass() {
        return BytesWritable.class;
    }

    @Override
    public int getSerializedLength(Object o) throws IOException {
        return length;
    }

    @Override
    public void serialize(Object o, ImmutableBytesWritable w)
            throws IOException {
        byte[] bytesToWriteIn = w.get();
        int writeOffset = w.getOffset();

        final BytesWritable bytesWritableToWrite = (BytesWritable) o;
        final int srcLen = bytesWritableToWrite.getLength();
        final byte[] bytesToWrite = bytesWritableToWrite.getBytes();

        if (srcLen != length)
            throw new IllegalArgumentException(
                    "can only serialize byte arrays of length " + length + ", not " + srcLen);

        // apply the sort order mask
        final byte[] maskedBytesToWrite = maskAll(bytesToWrite, order, 0, srcLen);

        Bytes.putBytes(bytesToWriteIn, writeOffset, maskedBytesToWrite, 0, srcLen);
        RowKeyUtils.seek(w, srcLen);
    }

    private byte[] maskAll(byte[] bytes, Order order, int offset, int length) {
        if (order.mask() == 0) {
            return bytes; // xor with zeroes has no effect anyways
        } else {
            final byte[] masked = new byte[bytes.length];
            for (int i = offset; i < length + offset; i++) {
                masked[i] = (byte) (bytes[i] ^ order.mask());
            }
            return masked;
        }
    }

    @Override
    public void skip(ImmutableBytesWritable w) throws IOException {
        RowKeyUtils.seek(w, length);
    }

    @Override
    public Object deserialize(ImmutableBytesWritable w) throws IOException {
        int offset = w.getOffset();
        byte[] serialized = w.get();

        final byte[] unmasked = maskAll(serialized, order, offset, length);

        RowKeyUtils.seek(w, length);

        final BytesWritable result = new BytesWritable();
        result.set(unmasked, offset, length);
        return result;
    }
}
