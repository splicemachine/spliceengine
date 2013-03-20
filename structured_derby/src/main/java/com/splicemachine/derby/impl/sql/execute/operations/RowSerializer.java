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
    private int[] colsToRowArrayMap;
    private final boolean appendPostfix;

    public RowSerializer(DataValueDescriptor[] rowTemplate,
                         FormatableBitSet cols,boolean appendPostfix) throws StandardException {
        this(rowTemplate,cols,null,appendPostfix);
    }

    public RowSerializer(DataValueDescriptor[] rowTemplate,
                         FormatableBitSet cols,
                         int[] colsToRowArrayMap,
                         boolean appendPostfix) {
        this.cols = cols;
        this.appendPostfix = appendPostfix;
        this.colsToRowArrayMap = colsToRowArrayMap;

        if(cols!=null &&appendPostfix)
            this.values = new Object[cols.getNumBitsSet()+1];
        else if(cols!=null)
            this.values = new Object[cols.getNumBitsSet()];
        else
            this.values = new Object[1];

        StructBuilder builder = new StructBuilder();
        if(this.cols!=null){
            for(int i= this.cols.anySetBit();i!=-1;i=this.cols.anySetBit(i)){
                RowKey rowKey;
                if(colsToRowArrayMap!=null)
                    rowKey = DerbyBytesUtil.getRowKey(rowTemplate[colsToRowArrayMap[i+1]]);
                else
                    rowKey = DerbyBytesUtil.getRowKey(rowTemplate[i]);
                builder.add(rowKey);
            }
            if(appendPostfix)
                builder.add(new VariableLengthByteArrayRowKey());
        }else{
            builder.add(new VariableLengthByteArrayRowKey());
        }
        rowKey = builder.toRowKey();
    }

    public byte[] serialize(DataValueDescriptor[] row) throws StandardException, IOException {
        int pos;
        if(cols!=null){
            pos =0;
            for(int i=cols.anySetBit();i!=-1;pos++,i=cols.anySetBit(i)){
                if(colsToRowArrayMap!=null)
                    values[pos] = DerbyBytesUtil.getObject(row[colsToRowArrayMap[i+1]]);
                else
                    values[pos] = DerbyBytesUtil.getObject(row[i]);
            }
            if(appendPostfix)
                values[pos] = SpliceUtils.getUniqueKey();
        }else{
            //we have no key columns, so just generate a unique key to begin with
            values[0] = SpliceUtils.getUniqueKey();
        }

        return rowKey.serialize(values);
    }
}
