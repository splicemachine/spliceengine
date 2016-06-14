package com.splicemachine.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents something that can be encoded into bytes, either directly on to
 * a stream or indirectly into a byte[] (depending on the caller's preference).
 *
 * @author Scott Fines
 *         Date: 2/24/15
 */
public interface Encodeable {

    void encode(DataOutput encoder) throws IOException;

    void decode(DataInput decoder) throws IOException;
}
