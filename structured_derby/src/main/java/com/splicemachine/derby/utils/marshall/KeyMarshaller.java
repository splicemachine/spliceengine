package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.Cell;

import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.CellUtils;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/21/14
 * Time: 8:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeyMarshaller {

    public void decode(Cell value, DataValueDescriptor[] fields,
                       int[] reversedKeyColumns, MultiFieldDecoder keyDecoder,
                       int[] columnOrdering, DataValueDescriptor[] kdvds) throws StandardException {
        //data is packed in the single row
        unpack(fields, reversedKeyColumns, keyDecoder, CellUtils.getBuffer(value), value.getRowOffset(),
                value.getRowLength(), columnOrdering, kdvds);
    }

    private void unpack(DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder keyDecoder,
                        byte[] data, int offset, int length, int[] columnOrdering, DataValueDescriptor[] kdvds)
            throws StandardException {
        keyDecoder.set(data, offset, length);
        for (int i = 0; i < columnOrdering.length; ++i) {
            int kcol = columnOrdering[i];
            if (kcol < reversedKeyColumns.length) {
                if (reversedKeyColumns[kcol] == -1) {
                    // skip this key column because it is not requested
                    DerbyBytesUtil.skip(keyDecoder,kdvds[i]);;
                }
                else {
                    DataValueDescriptor dvd = fields[reversedKeyColumns[kcol]];
                    DerbyBytesUtil.decodeInto(keyDecoder, dvd);
                }
            }
        }
    }
}
