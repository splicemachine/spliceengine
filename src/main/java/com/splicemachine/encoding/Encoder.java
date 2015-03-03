package com.splicemachine.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public interface Encoder<T> {

    void encode(T item,DataOutput dataInput) throws IOException;

    T decode(DataInput input) throws IOException;
}
