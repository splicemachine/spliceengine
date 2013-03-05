package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.HBaseConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class Constraints {
    private static final Constraint EMPTY_CONSTRAINT = new Constraint() {
        @Override
        public boolean validate(Put put) throws IOException {
            return true;
        }

        @Override
        public boolean validate(Delete delete) throws IOException {
            return true;
        }
    };

    private Constraints(){} //can't make me!

    public static byte[] getReferencedRowKey(Map<byte[],byte[]> dataMap,BitSet columns) throws IOException{
        //get the columns from the put
        byte[][] cols = new byte[columns.size()][];
        int size = 0;
        for(int fk=columns.nextSetBit(0),pos=0;fk!=-1;pos++,fk = columns.nextSetBit(fk+1)){
            byte[] value = dataMap.get(Integer.toString(fk).getBytes());
            if(value ==null){
                //the put is missing a value for the column, can't have that--validation fails right off
                return null;
            }
            cols[pos] = value;
            size+=value.length;
        }
        return constructCompositeKey(cols, size);
    }

    public static byte[] getReferencedRowKey(Put put, BitSet columns) throws IOException{
        //get the columns from the put
        byte[][] cols = new byte[columns.size()][];
        int size = 0;
        for(int fk=columns.nextSetBit(0),pos=0;fk!=-1;pos++,fk = columns.nextSetBit(fk+1)){
            List<KeyValue> values = put.get(HBaseConstants.DEFAULT_FAMILY_BYTES, Integer.toString(fk).getBytes());
            if(values ==null||values.isEmpty()){
                //the put is missing a value for the column, can't have that--validation fails right off
                return null;
            }
            cols[pos] = values.get(0).getBuffer();
            size+=cols[pos].length;
        }
        return constructCompositeKey(cols, size);
    }

    public static Constraint noConstraint(){
        return EMPTY_CONSTRAINT;
    }

     private static byte[] constructCompositeKey(byte[][] cols, int size) {
        byte[] finalRowKey = new byte[size];
        int offset=0;
        for(byte[] col:cols){
            System.arraycopy(col,0,finalRowKey,offset,col.length);
            offset+=col.length;
        }
        return finalRowKey;
    }
}
