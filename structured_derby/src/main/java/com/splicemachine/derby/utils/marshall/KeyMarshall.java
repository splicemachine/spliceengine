package com.splicemachine.derby.utils.marshall;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Created on: 6/12/13
 */
public interface KeyMarshall {
    public void encodeKey(DataValueDescriptor[] columns,
                          int[] keyColumns,
                          boolean[] sortOrder,
                          byte[] keyPostfix,
                          MultiFieldEncoder keyEncoder) throws StandardException;

    public void decode(DataValueDescriptor[] data,
                int[] reversedKeyColumns,
                boolean[] sortOrder,
                MultiFieldDecoder rowDecoder) throws StandardException;

    public int getFieldCount(int[] keyColumns);
}
