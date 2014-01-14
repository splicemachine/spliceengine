package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author Scott Fines
 * Created on: 6/12/13
 */
public class RowDecoder {
    private final KeyMarshall keyType;
    /* a mapping from HBase column position -> ExecRow position */
    private final int[] reversedKeyColumns;

    private MultiFieldDecoder keyDecoder;

    private final RowMarshall rowType;
    /* a mapping from HBase column position -> ExecRow position */
    private final int[] reversedRowColumns;
    private boolean clone;
    private final boolean[] sortOrder;

    private MultiFieldDecoder rowDecoder;

    private final ExecRow template;

    public static RowDecoder create(ExecRow template,
                                    KeyMarshall keyType,
                                    int[] keyColumns,
                                    boolean[] sortOrder,
                                    RowMarshall rowType,
                                    int[] rowColumns,
                                    boolean clone) {
        return new RowDecoder(template,keyType, keyColumns, sortOrder,rowType,rowColumns,clone);
    }

    RowDecoder(ExecRow template,
                       KeyMarshall keyType,
                       int[] reversedKeyColumns,
                       boolean[] sortOrder,
                       RowMarshall rowType,
                       int[] reversedRowColumns,
                       boolean clone) {
        this.keyType = keyType;
        this.rowType = rowType;
        this.template = template;
        this.sortOrder = sortOrder;
        this.reversedKeyColumns = reversedKeyColumns;
        this.reversedRowColumns = reversedRowColumns;
        this.clone = clone;

        if(reversedKeyColumns!=null && reversedKeyColumns.length>0){
            //some columns are stored in the row key, so need to initialize
            //a decoder for that
            keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
        }

        if(!rowType.isColumnar()){
            //our data is in a single column
            rowDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
        }
    }

    public ExecRow decode(Iterable<? extends KeyValue> keyValues) throws StandardException {
        ExecRow row = clone?template.getClone(): template;
        boolean rowSet = false;
        DataValueDescriptor[] fields = row.getRowArray();
        for(DataValueDescriptor field:fields){
            field.setToNull();
        }
        for(KeyValue value: keyValues){
            if(!rowSet){
                if(keyDecoder==null)
                    rowSet=true;
                else{
                    keyDecoder.set(value.getRow());
                    rowSet=true;
                }
            }
            rowType.decode(value,fields,reversedRowColumns,rowDecoder);
        }

        if(keyType!=null)
            keyType.decode(fields, reversedKeyColumns, sortOrder,keyDecoder);

        return row;
    }

    public ExecRow decode(KeyValue[] keyValues) throws StandardException{
        boolean rowSet = keyType == null; //if keyType!=null, we want to set on the keyDecoder, otherwise don't bother
        template.resetRowArray();
        DataValueDescriptor []fields = template.getRowArray();
        for(KeyValue value: keyValues){
            if(!rowSet){
                if(keyDecoder==null)
                    rowSet=true;
                else{
                    keyDecoder.set(value.getRow());
                    rowSet=true;
                }
            }
            rowType.decode(value,fields,reversedRowColumns,rowDecoder);
        }

        if(keyType!=null)
            keyType.decode(fields, reversedKeyColumns,sortOrder, keyDecoder);
        return template;
    }


    public int[] getKeyColumns() {
        return reversedKeyColumns;
    }

    public void reset() {
        if(keyDecoder!=null)
            keyDecoder.reset();
        if(rowDecoder!=null)
            rowDecoder.reset();
    }

    public ExecRow getTemplate() {
        return template;
    }

    public void close(){
        if(keyDecoder!=null)
            keyDecoder.close();
        if(rowDecoder!=null)
            rowDecoder.close();
    }
}
