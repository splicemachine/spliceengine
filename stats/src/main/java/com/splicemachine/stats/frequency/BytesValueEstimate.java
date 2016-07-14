/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stats.frequency;

import com.splicemachine.primitives.ByteComparator;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class BytesValueEstimate implements BytesFrequencyEstimate {
    private final ByteComparator byteComparator;
    private byte[] data;
    private long count;
    private long error;

    public BytesValueEstimate(byte[] data, long count, long error,ByteComparator byteComparator) {
        this.data = data;
        this.count = count;
        this.error = error;
        this.byteComparator = byteComparator;
    }

    @Override public ByteBuffer valueBuffer() { return ByteBuffer.wrap(data); }

    @Override public byte[] valueArrayBuffer() { return data; }
    @Override public int valueArrayLength() { return data.length; }
    @Override public int valueArrayOffset() { return 0; }

    @Override
    public int compare(ByteBuffer buffer) {
        return -1*byteComparator.compare(buffer,data,0,data.length);
    }

    @Override
    public int compare(byte[] buffer, int offset, int length) {
        return byteComparator.compare(data,0,data.length,buffer,offset,length);
    }

    @Override
    public int compareTo(BytesFrequencyEstimate o) {
        return compare(o.valueArrayBuffer(),o.valueArrayOffset(),o.valueArrayLength());
    }

    @Override public ByteBuffer getValue() { return valueBuffer(); }
    @Override public long count() { return count; }
    @Override public long error() { return error; }

    @Override
    public FrequencyEstimate<ByteBuffer> merge(FrequencyEstimate<ByteBuffer> other) {
        this.count+=other.count();
        this.error+=other.error();
        return this;
    }

    @Override
    public ByteComparator byteComparator() {
        return byteComparator;
    }
    //TODO -sf- do equals and hashCode methods
}
