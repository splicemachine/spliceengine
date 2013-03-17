
package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * Serialize and deserialize byte arrays into a fixed-length byte array.
 * <p/>
 * The serialization and deserialization methods are identical to {@link FixedBytesWritableRowKey} after converting the
 * BytesWritable to/from a byte[].
 */
public class FixedByteArrayRowKey extends FixedBytesWritableRowKey {

    public FixedByteArrayRowKey(int length) {
        super(length);
    }

    @Override
    public Class<?> getSerializedClass() {
        return byte[].class;
    }

    protected Object toBytesWritable(Object o) {
        if (o == null || o instanceof BytesWritable)
            return o;
        else {
            final BytesWritable bw = new BytesWritable();
            final byte[] bytes = (byte[]) o;
            bw.set(bytes, 0, bytes.length);
            return bw;
        }
    }

    @Override
    public int getSerializedLength(Object o) throws IOException {
        return super.getSerializedLength(toBytesWritable(o));
    }

    @Override
    public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
        super.serialize(toBytesWritable(o), w);
    }

    @Override
    public Object deserialize(ImmutableBytesWritable w) throws IOException {
        BytesWritable bw = (BytesWritable) super.deserialize(w);
        if (bw == null) {
            return null;
        }
        else {
            final byte[] result = new byte[bw.getLength()];
            System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength());

            return result;
        }
    }
}
