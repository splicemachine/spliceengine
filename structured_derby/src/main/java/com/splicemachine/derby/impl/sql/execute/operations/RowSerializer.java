package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.util.Random;

/**
 * Handles Serialization/Deserialization of a Row at a time
 *
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class RowSerializer {
    private FormatableBitSet cols;
    private int[] colsToRowArrayMap;
    private final boolean appendPostfix;
    private Random salter;
    private MultiFieldEncoder encoder;

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
            this.encoder = MultiFieldEncoder.create(cols.getNumBitsSet()+1);
        else if(cols!=null)
            this.encoder = MultiFieldEncoder.create(cols.getNumBitsSet());
        else
            this.encoder = MultiFieldEncoder.create(2);

//        StructBuilder builder = new StructBuilder();
//        if(this.cols!=null){
//            for(int i= this.cols.anySetBit();i!=-1;i=this.cols.anySetBit(i)){
//                RowKey rowKey;
//                if(colsToRowArrayMap!=null)
//                    rowKey = DerbyBytesUtil.getRowKey(rowTemplate[colsToRowArrayMap[i+1]]);
//                else
//                    rowKey = DerbyBytesUtil.getRowKey(rowTemplate[i]);
//                builder.add(rowKey);
//            }
//            if(appendPostfix)
//                builder.add(new VariableLengthByteArrayRowKey());
//        }else{
//            builder.add(new IntegerRowKey());
//            builder.add(new VariableLengthByteArrayRowKey());
//        }
//        rowKey = builder.toRowKey();
    }

    public byte[] serialize(DataValueDescriptor[] row) throws StandardException, IOException {
        encoder.reset();

        int pos;
        if(cols!=null){
            pos =0;
            for(int i=cols.anySetBit();i!=-1;pos++,i=cols.anySetBit(i)){
                if(colsToRowArrayMap!=null)
                    encoder = DerbyBytesUtil.encodeInto(encoder,row[colsToRowArrayMap[i+1]],false);
                else
                    encoder = DerbyBytesUtil.encodeInto(encoder,row[i],false);
            }
            if(appendPostfix)
                encoder = encoder.encodeNext(SpliceUtils.getUniqueKey());
        }else{
            /*
             * There are no columns to use to construct the key out of, which is handy, we
             * can just generate a unique key and move on.
             *
             * However, for good HBase dispersion, we can't just use a UUID, as they are generally
             * ordered within the same JVM (Ordered keys means sending writes to the same region, which
             * prevents good concurrent writes), so we throw on a 4-byte salt to the beginning.
             */
            if(salter==null)
                salter = new Random(System.currentTimeMillis());

            encoder.encodeNext(salter.nextInt());
            encoder.encodeNext(SpliceUtils.getUniqueKey());
        }

        return encoder.build();
    }
}
