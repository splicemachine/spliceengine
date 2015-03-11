package com.splicemachine.derby.utils.marshall;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 6/12/13
 */
public interface RowMarshall {
    /**
     * @param row the row to encode
     * @param rowColumns the columns to parse
     * @param rowEncoder {@code null} if the row type does not use multi field encodings
     * @throws com.splicemachine.db.iapi.error.StandardException
     */
    public byte[] encodeRow(DataValueDescriptor[] row,
                          int[] rowColumns,
                          MultiFieldEncoder rowEncoder) throws StandardException;

    public void encodeKeyValues(DataValueDescriptor[] row,
                                byte[] rowKey,
                                int[] rowColumns,
                                MultiFieldEncoder rowEncoder,
                                List<KeyValue> kvResults) throws StandardException;

    public void fill(DataValueDescriptor[] row,int[] rowColumns,MultiFieldEncoder encoder) throws StandardException;

    void decode(KeyValue value,
                DataValueDescriptor[] fields,
                int[] reversedKeyColumns,
                MultiFieldDecoder rowDecoder) throws StandardException;

    void decode(KeyValue value,
                DataValueDescriptor[] fields,
                int[] reversedKeyColumns,
                EntryDecoder entryDecoder) throws StandardException;

    boolean isColumnar();
}
