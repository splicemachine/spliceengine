package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.BitSet;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/21/14
 * Time: 8:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeyMarshaller {
    private BitSet descColumns;

    public KeyMarshaller() {this.descColumns = null;}
    public KeyMarshaller (BitSet descColumns) {
        this.descColumns = descColumns;
    }

    public void decode(KeyValue value, DataValueDescriptor[] fields,
                       int[] reversedKeyColumns, MultiFieldDecoder keyDecoder,
                       int[] columnOrdering, DataValueDescriptor[] kdvds) throws StandardException {
        //data is packed in the single row
        unpack(fields, reversedKeyColumns, keyDecoder, value.getBuffer(), value.getRowOffset(),
                value.getRowLength(), columnOrdering, kdvds);
    }

    private void unpack(DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder keyDecoder,
                        byte[] data, int offset, int length, int[] columnOrdering, DataValueDescriptor[] kdvds)
            throws StandardException {
        keyDecoder.set(data, offset, length);
        for (int i = 0; i < columnOrdering.length; ++i) {
            int kcol = columnOrdering[i];
            if (kcol < reversedKeyColumns.length && reversedKeyColumns[kcol] != -1) {
                DataValueDescriptor dvd = fields[reversedKeyColumns[kcol]];
                DerbyBytesUtil.decodeInto(keyDecoder, dvd, isDescColumn(kcol));
            }
            else {
                // skip this key column because it is not requested
                // but don't need to skip the last column
                if (i != columnOrdering.length - 1) {
                    DerbyBytesUtil.skip(keyDecoder, kdvds[i]);
                }
            }
        }
    }

    private boolean isDescColumn(int i) {
        return (descColumns != null && descColumns.get(i));
    }
}
