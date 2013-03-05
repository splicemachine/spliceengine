package com.splicemachine.derby.impl.sql.execute.operations;

import com.gotometrics.orderly.*;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * Handles Serialization/Deserialization of a Row at a time
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class RowSerializer {
    private FormatableBitSet cols;
    private Object[] values;
    private StructRowKey rowKey;
    private final boolean appendPostfix;

    public RowSerializer(DataValueDescriptor[] rowTemplate,
                         FormatableBitSet cols,boolean appendPostfix) throws StandardException {
        this.cols = cols;
        this.appendPostfix = appendPostfix;
        if(cols!=null &&appendPostfix)
            this.values = new Object[cols.getNumBitsSet()+1];
        else if(cols!=null)
            this.values = new Object[cols.getNumBitsSet()];
        else if(appendPostfix)
            this.values = new Object[rowTemplate.length+1];
        else
            this.values = new Object[rowTemplate.length];

        StructBuilder builder = new StructBuilder();
        if(this.cols!=null){
            for(int i= cols.anySetBit();i!=-1;i=cols.anySetBit(i)){
                RowKey rowKey = DerbyBytesUtil.getRowKey(rowTemplate[i]);
                builder.add(rowKey);
            }
        }else{
            for(DataValueDescriptor col:rowTemplate){
                builder.add(DerbyBytesUtil.getRowKey(col));
            }
        }
        if(appendPostfix)
            builder.add(new VariableLengthByteArrayRowKey());
        rowKey = builder.toRowKey();
    }

    public byte[] serialize(DataValueDescriptor[] row) throws StandardException, IOException {
        int pos;
        if(cols!=null){
            pos =0;
            for(int i=cols.anySetBit();i!=-1;pos++,i=cols.anySetBit(i)){
                values[pos] = DerbyBytesUtil.getObject(row[i]);
            }
        }else{
            pos=0;
            for(DataValueDescriptor col:row){
                values[pos] = DerbyBytesUtil.getObject(col);
                pos++;
            }
        }
        if(appendPostfix)
            values[pos] = SpliceUtils.getUniqueKey();

        return rowKey.serialize(values);
    }
}
