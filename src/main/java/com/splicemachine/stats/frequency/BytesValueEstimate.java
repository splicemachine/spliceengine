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

    @Override public byte[] getValue() { return data; }
    @Override public long count() { return count; }
    @Override public long error() { return error; }

    @Override
    public FrequencyEstimate<byte[]> merge(FrequencyEstimate<byte[]> other) {
        this.count+=other.count();
        this.error+=other.error();
        return this;
    }

    //TODO -sf- do equals and hashCode methods
}
