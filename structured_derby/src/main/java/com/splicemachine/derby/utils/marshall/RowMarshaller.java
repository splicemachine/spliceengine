package com.splicemachine.derby.utils.marshall;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 6/21/13
 */
public class RowMarshaller {
    public static final byte[] PACKED_COLUMN_KEY = SIConstants.PACKED_COLUMN_BYTES;

    private static KeyValue getPackedKv(DataValueDescriptor[] row,
                                        byte[] rowKey,
                                        int[] rowColumns,
                                        MultiFieldEncoder encoder, boolean encodeEmpty) throws StandardException{
        pack(row,rowColumns,encoder,encodeEmpty);
        return new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,encoder.build());
    }

    private static void pack(DataValueDescriptor[] row, int[] rowColumns,MultiFieldEncoder encoder,boolean encodeEmpty) throws StandardException{

        if(rowColumns!=null){
            for(int rowCol:rowColumns){
                if (rowCol == -1) continue;
                DataValueDescriptor dvd = row[rowCol];
                if(dvd==null){
                    if(encodeEmpty)
                        encoder.encodeEmpty();
                }else if(!dvd.isNull())
                    DerbyBytesUtil.encodeInto(encoder,dvd,false);
                else if(encodeEmpty){
                    DerbyBytesUtil.encodeTypedEmpty(encoder,dvd,false,true);
                }
            }
        }else{
            for (DataValueDescriptor dvd : row) {
                if(dvd==null ){
                   if(encodeEmpty)
                       encoder.encodeEmpty();
                }else if(!dvd.isNull())
                    DerbyBytesUtil.encodeInto(encoder,dvd,false);
                else if(encodeEmpty){
                    DerbyBytesUtil.encodeTypedEmpty(encoder,dvd,false,true);
                }
            }
        }
    }

    private static final RowMarshall SPARSE_PACKED = new RowMarshall() {
        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] columns,EntryDecoder entryDecoder) throws StandardException {
            if(!CellUtils.singleMatchingColumn(value, SpliceConstants.DEFAULT_FAMILY_BYTES, PACKED_COLUMN_KEY)) return;
            entryDecoder.set(value.getBuffer(),value.getValueOffset(),value.getValueLength());
            unpack(fields, columns,entryDecoder);
        }

        @Override
        public byte[] encodeRow(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder rowEncoder) throws StandardException {
                pack(row,rowColumns,rowEncoder,false);
                return rowEncoder.build();
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            kvResults.add(getPackedKv(row,rowKey,rowColumns,rowEncoder,false));
        }

        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            pack(row,rowColumns,encoder,false);
        }

        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder) throws StandardException {
            //data is packed in the single value
            if(!CellUtils.singleMatchingColumn(value, SpliceConstants.DEFAULT_FAMILY_BYTES, PACKED_COLUMN_KEY)) return;
            unpack(fields, reversedKeyColumns, rowDecoder, value.getBuffer(), value.getValueOffset(), value.getValueLength());
        }

        @Override
        public boolean isColumnar() {
            return false;
        }
    };



    private static final RowMarshall PACKED = new RowMarshall() {
        @Override
        public byte[] encodeRow(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder rowEncoder) throws StandardException {
            pack(row,rowColumns,rowEncoder,true);
            return rowEncoder.build();
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            kvResults.add(getPackedKv(row,rowKey,rowColumns,rowEncoder,true));
        }

        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            pack(row,rowColumns,encoder,true);
        }

        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder) throws StandardException {
            //data is packed in the single value
            if(!CellUtils.singleMatchingColumn(value, SpliceConstants.DEFAULT_FAMILY_BYTES, PACKED_COLUMN_KEY)) return;
            unpack(fields, reversedKeyColumns, rowDecoder, value.getBuffer(),value.getValueOffset(), value.getValueLength());
        }

        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] columns, EntryDecoder entryDecoder) throws StandardException {
            if(!CellUtils.singleMatchingColumn(value, SpliceConstants.DEFAULT_FAMILY_BYTES, PACKED_COLUMN_KEY)) return;
            entryDecoder.set(value.getBuffer(),value.getValueOffset(),value.getValueLength());
            unpack(fields,columns,entryDecoder);
        }

        @Override
        public boolean isColumnar() {
            return false;
        }
    };

    private static void unpack(DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder, byte[] data, int offset, int length) throws StandardException {
        rowDecoder.set(data, offset, length);
        if(reversedKeyColumns!=null){
            for(int keyCol:reversedKeyColumns){
                DataValueDescriptor dvd = fields[keyCol];
                if (DerbyBytesUtil.isNextFieldNull(rowDecoder,dvd)) {
                    dvd.setToNull();
                    DerbyBytesUtil.skip(rowDecoder,dvd);
                }else
                    DerbyBytesUtil.decodeInto(rowDecoder, dvd);
            }
        }else{
            for (DataValueDescriptor dvd : fields) {
                if (dvd == null)
                    continue;
                if (DerbyBytesUtil.isNextFieldNull(rowDecoder,dvd)) {
                    dvd.setToNull();
                    DerbyBytesUtil.skip(rowDecoder,dvd);
                } else
                    DerbyBytesUtil.decodeInto(rowDecoder, dvd);
            }
        }
    }

    private static void unpack(DataValueDescriptor[] fields, int[] reversedKeyColumns,EntryDecoder entryDecoder) throws StandardException {
        BitIndex index = entryDecoder.getCurrentIndex();
        MultiFieldDecoder decoder;
        try {
            decoder = entryDecoder.getEntryDecoder();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if(reversedKeyColumns!=null){
            for(int i=index.nextSetBit(0);i>=0 && i<reversedKeyColumns.length;i=index.nextSetBit(i+1)){
                int pos = reversedKeyColumns[i];
                if (pos == -1 || pos > fields.length)
                {
                    entryDecoder.seekForward(decoder,i);
                    continue;
                }
                DataValueDescriptor dvd = fields[pos];
                if(dvd==null){
                    entryDecoder.seekForward(decoder,i);
                    continue;
                }
                if(DerbyBytesUtil.isNextFieldNull(decoder,dvd)){
                    dvd.setToNull();
                    DerbyBytesUtil.skip(decoder,dvd);
                }else{
                    DerbyBytesUtil.decodeInto(decoder,dvd);
                }
            }
        }else{
            for(int i=index.nextSetBit(0);i>=0 && i<fields.length;i=index.nextSetBit(i+1)){
                DataValueDescriptor dvd = fields[i];
                if(dvd==null){
                    entryDecoder.seekForward(decoder,i);
                    continue;
                }if(DerbyBytesUtil.isNextFieldNull(decoder,dvd)){
                    DerbyBytesUtil.skip(decoder,dvd);
                }else{
                    DerbyBytesUtil.decodeInto(decoder,dvd);
                }
            }
        }
    }

    public static RowMarshall packed(){
        return PACKED;
    }

    public static RowMarshall sparsePacked() {
        return SPARSE_PACKED;
    }
}
