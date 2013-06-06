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

package orderly;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * Serialize and deserialize byte arrays into a variable-length byte array.
 * <p/>
 * The serialization and deserialization methods are identical to {@link orderly.VariableLengthBytesWritableRowKey}
 * after converting the BytesWritable to/from a byte[].
 */
public class VariableLengthByteArrayRowKey extends VariableLengthBytesWritableRowKey {

    public VariableLengthByteArrayRowKey() {
    }

    public VariableLengthByteArrayRowKey(int fixedPrefixLength) {
        super(fixedPrefixLength);
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
        } else {
            final byte[] result = new byte[bw.getLength()];
            System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength());

            return result;
        }
    }
}
