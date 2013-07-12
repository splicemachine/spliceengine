package com.splicemachine.derby.utils;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public class EncodingUtils {

    public static BitSet getNonNullColumns(DataValueDescriptor[] row,FormatableBitSet validColumns) {
        BitSet setCols = new BitSet(row.length);
        if(validColumns!=null){
            for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
                if(row[i]!=null && !row[i].isNull())
                    setCols.set(i);
            }
        }
        else{
            for(int i=0;i<row.length;i++){
                DataValueDescriptor dvd = row[i];
                if(dvd!=null && !dvd.isNull())
                    setCols.set(i);
            }
        }
        return setCols;
    }

    public static void encodeRow(DataValueDescriptor[] row, Put put,int[] columns,FormatableBitSet validColumns,EntryEncoder encoder) throws StandardException, IOException {
        BitSet lengthFields = DerbyBytesUtil.getLengthDelimitedFields(row);
        encoder.reset(getNonNullColumns(row,validColumns),lengthFields);

        RowMarshaller.sparsePacked().fill(row, columns, encoder.getEntryEncoder());
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY, encoder.encode());
    }
}
