package com.splicemachine.derby.utils.marshall;

import com.google.common.base.Preconditions;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 6/12/13
 */
public class RowEncoder {

    protected final MultiFieldEncoder keyEncoder;

    protected final int[] keyColumns;
    protected final boolean[] keySortOrder;

    protected final KeyMarshall keyType;
    protected final RowMarshall rowType;

    protected byte[] keyPostfix;

    /*map of columns which are NOT contained in the key*/
    protected final int[] rowColumns;
    protected MultiFieldEncoder rowEncoder;

    protected RowEncoder(int[] keyColumns,
                       boolean[] keySortOrder,
                       int[] rowColumns,
                       byte[] keyPrefix,
                       KeyMarshall keyType,
                       RowMarshall rowType){
        this.keyColumns = keyColumns;
        this.keySortOrder = keySortOrder;
        this.keyType = keyType;
        this.rowType = rowType;
        this.rowColumns = rowColumns;

        int encodedCols = keyType.getFieldCount(keyColumns);
        this.keyEncoder = MultiFieldEncoder.create(encodedCols);
        if(keyType==KeyType.FIXED_PREFIX ||
                keyType==KeyType.FIXED_PREFIX_AND_POSTFIX
                ||keyType==KeyType.FIXED_PREFIX_UNIQUE_POSTFIX
                ||keyType==KeyType.PREFIX_FIXED_POSTFIX_ONLY
                ||keyType==KeyType.PREFIX_UNIQUE_POSTFIX_ONLY){
            Preconditions.checkNotNull(keyPrefix,
                    "No Key Prefix specified, but KeyType.FIXED_PREFIX chosen");
            //set the prefix and mark
            keyEncoder.setRawBytes(keyPrefix);
            keyEncoder.mark();
        }else if(keyPrefix!=null){
            keyEncoder.setRawBytes(keyPrefix);
            keyEncoder.mark();
        }

        if(!rowType.isColumnar()){
            rowEncoder = MultiFieldEncoder.create(rowColumns.length);
        }
    }

    public static RowEncoder create(int numCols,
                                    int[] keyColumns,
                                    boolean[] keySortOrder,
                                    byte[] keyPrefix,
                                    KeyMarshall keyType,
                                    RowMarshall rowType){
        //get the rowColumn Positions
        //TODO -sf- can we do this without the extra int[]?
        if(keyColumns==null)
            keyColumns = new int[0];

        int[] allCols = new int[numCols];
        for (int keyCol : keyColumns) {
            allCols[keyCol] = -1;
        }

        int[] rowColumns = new int[numCols-keyColumns.length];
        int pos = 0;
        for(int rowPos=0;rowPos<allCols.length;rowPos++){
            if(allCols[rowPos]!=-1){
                rowColumns[pos] = rowPos;
                pos++;
            }
        }

        return new RowEncoder(keyColumns,keySortOrder,rowColumns,keyPrefix,keyType,rowType);
    }

    public static RowEncoder createEntryEncoder(int numCols,
                                                int[] keyColumns,
                                                boolean[] keySortOrder,
                                                byte[] keyPrefix,
                                                KeyMarshall keyType,
                                                BitSet scalarFields,
                                                BitSet floatFields,
                                                BitSet doubleFields){
        if(keyColumns==null)
            keyColumns = new int[0];

        int[] rowCols = new int[numCols];
        for(int rowPos=0;rowPos<numCols;rowPos++){
            rowCols[rowPos] = rowPos;
        }

        return new EntryRowEncoder(keyColumns,keySortOrder,rowCols,keyPrefix,keyType,scalarFields,floatFields,doubleFields);
    }

    public static RowEncoder createDoubleWritingEncoder(int numCols,
                                                        int[] keyColumns,
                                                        boolean[] keySortOrder,
                                                        byte[] keyPrefix,
                                                        KeyMarshall keyType,
                                                        RowMarshall rowType){
        if(keyColumns==null)
            keyColumns = new int[0];

        int[] rowCols = new int[numCols];
        for(int rowPos=0;rowPos<numCols;rowPos++){
            rowCols[rowPos] = rowPos;
        }
        return new RowEncoder(keyColumns,keySortOrder,rowCols,keyPrefix,keyType,rowType);
    }

    public static RowEncoder createDeleteEncoder(final String txnId,KeyMarshall keyMarshall){

       return new RowEncoder(new int[0],null,new int[]{},null,keyMarshall,RowMarshaller.columnar()){
           @Override
           protected Put doPut(ExecRow row) throws StandardException {
               //construct the row key
               keyEncoder.reset();
               keyType.encodeKey(row.getRowArray(), keyColumns, keySortOrder, keyPostfix, keyEncoder);

               return SpliceUtils.createDeletePut(txnId,keyEncoder.build());
           }
       };
    }

    public RowDecoder getDual(ExecRow template){
        return new RowDecoder(template,keyType,keyColumns,keySortOrder,rowType,rowColumns,false);
    }

    public RowDecoder getDual(ExecRow template,boolean cloneRow){
        return new RowDecoder(template,keyType,keyColumns,keySortOrder,rowType,rowColumns,cloneRow);
    }

    public void setPostfix(byte[] postfix) {
        this.keyPostfix = postfix;
    }

    protected Put doPut(ExecRow row) throws StandardException{
        //construct the row key
        keyEncoder.reset();
        keyType.encodeKey(row.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);
        Put put = new Put(keyEncoder.build());

        if(rowEncoder!=null)
            rowEncoder.reset();

        rowType.encodeRow(row.getRowArray(),rowColumns,put,rowEncoder);

        return put;
    }

    public void write(ExecRow row,String txnId,CallBuffer<Mutation> buffer) throws Exception{
        Put element = doPut(row);
        SpliceUtils.attachTransaction(element,txnId);
        buffer.add(element);
    }

    private static class EntryRowEncoder extends RowEncoder{
        private EntryEncoder entryEncoder;

        protected EntryRowEncoder(int[] keyColumns,
                                  boolean[] keySortOrder,
                                  int[] rowColumns,
                                  byte[] keyPrefix,
                                  KeyMarshall keyType,
                                  BitSet scalarFields,
                                  BitSet floatFields,
                                  BitSet doubleFields) {
            super(keyColumns, keySortOrder, rowColumns, keyPrefix, keyType, RowMarshaller.sparsePacked());

            BitSet rowCols = new BitSet();
            for(int rowCol:rowColumns){
                rowCols.set(rowCol);
            }

            this.entryEncoder = EntryEncoder.create(rowColumns.length,rowCols,scalarFields,floatFields,doubleFields);
            rowEncoder = entryEncoder.getEntryEncoder();
        }

        @Override
        protected Put doPut(ExecRow row) throws StandardException {
            //construct the row key
            keyEncoder.reset();
            keyType.encodeKey(row.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);
            Put put = new Put(keyEncoder.build());

            if(rowEncoder!=null)
                rowEncoder.reset();

            rowType.fill(row.getRowArray(), rowColumns, rowEncoder);

            try {
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,entryEncoder.encode());
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
            return put;
        }
    }
}
