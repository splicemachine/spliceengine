package com.splicemachine.derby.utils.marshall;

import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
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
            keyDecoder = MultiFieldDecoder.create();
        }

        if(rowType!=RowType.COLUMNAR){
            //our data is in a single column
            rowDecoder = MultiFieldDecoder.create();
        }
    }

    public ExecRow decode(Iterable<? extends KeyValue> keyValues) throws StandardException {
        ExecRow row = clone?template.getClone(): template;
        boolean rowSet = false;
        for(KeyValue value: keyValues){
            if(!rowSet){
                if(keyDecoder==null)
                    rowSet=true;
                else{
                    keyDecoder.set(value.getRow());
                    rowSet=true;
                }
            }
            rowType.decode(value,row,reversedKeyColumns,rowDecoder);
        }

        keyType.decode(row, reversedKeyColumns, sortOrder,keyDecoder);
        return row;
    }

    public ExecRow decode(KeyValue[] keyValues) throws StandardException{
        boolean rowSet = false;
        for(KeyValue value: keyValues){
            if(!rowSet){
                if(keyDecoder==null)
                    rowSet=true;
                else{
                    keyDecoder.set(value.getRow());
                    rowSet=true;
                }
            }
            rowType.decode(value,template,reversedRowColumns,rowDecoder);
        }

        keyType.decode(template, reversedKeyColumns,sortOrder, keyDecoder);
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
}
