package com.splicemachine.derby.utils.marshall;

import java.io.IOException;
import java.util.BitSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.google.common.base.Preconditions;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.HashUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryEncoder;

/**
 * @author Scott Fines
 * Created on: 6/12/13
 */
public class RowEncoder {
    public static final byte[] EMPTY_BYTES = new byte[]{};
    protected final MultiFieldEncoder keyEncoder;

    protected final int[] keyColumns;
    protected final boolean[] keySortOrder;

    protected final KeyMarshall keyType;
    protected final RowMarshall rowType;

    protected byte[] keyPostfix;

    /*map of columns which are NOT contained in the key*/
    protected final int[] rowColumns;
    protected MultiFieldEncoder rowEncoder;
    private byte[] hash;
    private boolean bucketed;

    public RowEncoder(int[] keyColumns,
                      boolean[] keySortOrder,
                      int[] rowColumns,
                      byte[] keyPrefix,
                      KeyMarshall keyType,
                      RowMarshall rowType){
        this(keyColumns, keySortOrder, rowColumns, keyPrefix, keyType, rowType, false);
    };

    public RowEncoder(int[] keyColumns,
                      boolean[] keySortOrder,
                      int[] rowColumns,
                      byte[] keyPrefix,
                      KeyMarshall keyType,
                      RowMarshall rowType,
                      boolean bucketed){
        this.keyColumns = keyColumns;
        this.keySortOrder = keySortOrder;
        this.keyType = keyType;
        this.rowType = rowType;
        this.rowColumns = rowColumns;
        this.bucketed = bucketed;

        int encodedCols = keyType.getFieldCount(keyColumns);
        if (bucketed) {
            this.keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),encodedCols + 1);
            this.hash = new byte[1]; // one-byte hash prefix
            keyEncoder.setRawBytes(this.hash); // we set it as first field, we rely on being able to update it later
                                               // it has to be stored as a reference to this.hash
        } else {
            this.keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),encodedCols);
        }
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

        if(!rowType.isColumnar()&&rowColumns!=null){
            rowEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),rowColumns.length);
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

    public static RowEncoder createDeleteEncoder(KeyMarshall keyMarshall){

       return new RowEncoder(new int[0],null,new int[]{},null,keyMarshall,RowMarshaller.sparsePacked()){
           @Override
           protected KVPair doPut(ExecRow row) throws StandardException {
               //construct the row key
               keyEncoder.reset();
               keyType.encodeKey(row.getRowArray(), keyColumns, keySortOrder, keyPostfix, keyEncoder);

               return new KVPair(keyEncoder.build(),EMPTY_BYTES, KVPair.Type.DELETE);
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

    protected KVPair doPut(ExecRow row) throws StandardException{
        //construct the row key
        keyEncoder.reset();
        keyType.encodeKey(row.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);
        updateHash();
        byte[] rowKey = keyEncoder.build();

        if(rowEncoder!=null)
            rowEncoder.reset();

        byte[] value = rowType.encodeRow(row.getRowArray(), rowColumns, rowEncoder);

        return new KVPair(rowKey,value);
    }

    private void updateHash() {
        if (!bucketed) {
            return;
        }
        byte[][] fields = new byte[keyColumns.length + 1][];
        // 0 is the hash byte
        // 1 is the UUID
        // 2 - N are the key fields
        for (int i = 0; i <= keyColumns.length; i++) {
            fields[i] = keyEncoder.getEncodedBytes(i + 1); // UUID            
        }
        this.hash[0] = HashUtils.hash(fields);
    }

    public void write(ExecRow row,CallBuffer<KVPair> buffer) throws Exception{
        KVPair element = doPut(row);
        buffer.add(element);
    }

    public void close() {
        if(rowEncoder!=null)
            rowEncoder.close();
        if(keyEncoder!=null)
            keyEncoder.close();
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

            this.entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),rowColumns.length,rowCols,scalarFields,floatFields,doubleFields);
            rowEncoder = entryEncoder.getEntryEncoder();
        }

        @Override
        protected KVPair doPut(ExecRow row) throws StandardException {
            //construct the row key
            keyEncoder.reset();
            keyType.encodeKey(row.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);
            byte[] rowKey = keyEncoder.build();

            if(rowEncoder!=null)
                rowEncoder.reset();


            //TODO -sf- more elegant way of doing this reset is needed
            BitSet setFields = DerbyBytesUtil.getNonNullFields(row.getRowArray());
//            BitSet scalarFields = DerbyBytesUtil.getScalarFields(row.getRowArray());
//            BitSet floatFields = DerbyBytesUtil.getFloatFields(row.getRowArray());
//            BitSet doubleFields = DerbyBytesUtil.getDoubleFields(row.getRowArray());
            entryEncoder.reset(setFields);
            rowEncoder = entryEncoder.getEntryEncoder();
            rowType.fill(row.getRowArray(), rowColumns, rowEncoder);

            try {
                byte[] value = entryEncoder.encode();
                return new KVPair(rowKey,value);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public void close() {
            if(entryEncoder!=null)
                entryEncoder.close();
        }
    }
}
