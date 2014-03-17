package com.splicemachine.derby.utils.marshall;

import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.Cell;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryDecoder;

/**
 * @author Scott Fines
 *         Created on: 6/12/13
 */
public interface RowMarshall {
    /**
     * @param row the row to encode
     * @param rowColumns the columns to parse
     * @param rowEncoder {@code null} if the row type does not use multi field encodings
     * @throws org.apache.derby.iapi.error.StandardException
     */
    public byte[] encodeRow(DataValueDescriptor[] row,
                          int[] rowColumns,
                          MultiFieldEncoder rowEncoder) throws StandardException;

    public void encodeKeyValues(DataValueDescriptor[] row,
                                byte[] rowKey,
                                int[] rowColumns,
                                MultiFieldEncoder rowEncoder,
                                List<Cell> kvResults) throws StandardException;

    public void fill(DataValueDescriptor[] row,int[] rowColumns,MultiFieldEncoder encoder) throws StandardException;

    void decode(Cell value,
                DataValueDescriptor[] fields,
                int[] reversedKeyColumns,
                MultiFieldDecoder rowDecoder) throws StandardException;

    void decode(Cell value,
                DataValueDescriptor[] fields,
                int[] reversedKeyColumns,
                EntryDecoder entryDecoder) throws StandardException;

    boolean isColumnar();
}
