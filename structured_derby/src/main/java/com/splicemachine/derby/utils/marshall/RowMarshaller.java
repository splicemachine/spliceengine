package com.splicemachine.derby.utils.marshall;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 6/21/13
 */
public class RowMarshaller {
    public static final byte[] PACKED_COLUMN_KEY = new byte[]{0x00};
    private static final byte[] EMPTY_ROW_COLUMN = Encoding.encode(-101);

    private static KeyValue getPackedKv(DataValueDescriptor[] row,
                                        byte[] rowKey,
                                        int[] rowColumns,
                                        MultiFieldEncoder encoder, boolean encodeEmpty) throws StandardException{
        pack(row,rowColumns,encoder,encodeEmpty);
        return new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,encoder.build());
    }

    private static void pack(DataValueDescriptor[] row, int[] rowColumns,MultiFieldEncoder encoder,boolean encodeEmpty) throws StandardException{
//        if(rowColumns!=null){
//            for(int rowCol:rowColumns){
//                DataValueDescriptor dvd = row[rowCol];
//                if(dvd==null && encodeEmpty)
//                    encoder.encodeEmpty();
//                else if (dvd!=null){
//                    DerbyBytesUtil.encodeInto(encoder,dvd,false,encodeEmpty);
//                }
//            }
//        }else{
//            for (DataValueDescriptor dvd : row) {
//                if(dvd==null&&encodeEmpty)
//                    encoder.encodeEmpty();
//                else if(dvd!=null){
//                    DerbyBytesUtil.encodeInto(encoder,dvd,false,encodeEmpty);
//                }
//            }
//        }

        if(rowColumns!=null){
            for(int rowCol:rowColumns){
                DataValueDescriptor dvd = row[rowCol];
                if(dvd!=null&&!dvd.isNull())
                    DerbyBytesUtil.encodeInto(encoder,dvd,false);
                else if (encodeEmpty)
                    encoder.encodeEmpty();
            }
        }else{
            for (DataValueDescriptor dvd : row) {
                if (dvd != null && !dvd.isNull())
                    DerbyBytesUtil.encodeInto(encoder, dvd, false);
                else if (encodeEmpty)
                    encoder.encodeEmpty();
            }
        }
    }

    private static final RowMarshall SPARSE_PACKED = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row, int[] rowColumns, Put put, MultiFieldEncoder rowEncoder) throws StandardException {
            try {
                put.add(getPackedKv(row,put.getRow(),rowColumns,rowEncoder,false));
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
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
            if(Bytes.compareTo(value.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            else if(Bytes.compareTo(PACKED_COLUMN_KEY,value.getQualifier())!=0)
                return; //don't try to unpack unless it's the right column

            byte[] data = value.getValue();
            unpack(fields, reversedKeyColumns, rowDecoder, data);
        }

        @Override
        public boolean isColumnar() {
            return false;
        }
    };
    private static final RowMarshall PACKED = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row, int[] rowColumns, Put put, MultiFieldEncoder rowEncoder) throws StandardException {
            try {
                put.add(getPackedKv(row,put.getRow(),rowColumns,rowEncoder,true));
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
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
            if(Bytes.compareTo(value.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            else if(Bytes.compareTo(PACKED_COLUMN_KEY,value.getQualifier())!=0)
                return; //don't try to unpack unless it's the right column

            byte[] data = value.getValue();
            unpack(fields, reversedKeyColumns, rowDecoder, data);
        }

        @Override
        public boolean isColumnar() {
            return false;
        }
    };

    private static void unpack(DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder, byte[] data) throws StandardException {
        rowDecoder.set(data);
        if(reversedKeyColumns!=null){
            for(int keyCol:reversedKeyColumns){
                DataValueDescriptor dvd = fields[keyCol];
                if(rowDecoder.nextIsNull()){
                    dvd.setToNull();
                    rowDecoder.skip();
                }else
                    DerbyBytesUtil.decodeInto(rowDecoder, dvd);
            }
        }else{
            for (DataValueDescriptor dvd : fields) {
                if (dvd == null)
                    continue;
                if (rowDecoder.nextIsNull()) {
                    dvd.setToNull();
                    rowDecoder.skip();
                } else
                    DerbyBytesUtil.decodeInto(rowDecoder, dvd);
            }
        }
//        if(reversedKeyColumns!=null){
//            for(int keyCol:reversedKeyColumns){
//                DataValueDescriptor dvd = fields[keyCol];
//                if(DerbyBytesUtil.isNextFieldNull(rowDecoder, dvd)){
//                    DerbyBytesUtil.skip(rowDecoder,dvd);
//                }else
//                    DerbyBytesUtil.decodeInto(rowDecoder,dvd);
//            }
//        }else{
//            for (DataValueDescriptor dvd : fields) {
//                if (dvd == null)
//                    continue;
//                if (DerbyBytesUtil.isNextFieldNull(rowDecoder, dvd)) {
//                    DerbyBytesUtil.skip(rowDecoder, dvd);
//                } else
//                    DerbyBytesUtil.decodeInto(rowDecoder, dvd);
//            }
//        }
    }

    private static final RowMarshall PACKED_COMPRESSED = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row,
                              int[] rowColumns,
                              Put put, MultiFieldEncoder rowEncoder) throws StandardException {
            pack(row,rowColumns,rowEncoder,true);
            try {
                byte[] bytes = Snappy.compress(rowEncoder.build());
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,bytes);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            pack(row,rowColumns,rowEncoder,true);
            try {
                byte[] bytes = Snappy.compress(rowEncoder.build());
                kvResults.add(new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,bytes));
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            pack(row,rowColumns,encoder,true);
        }

        @Override
        public void decode(KeyValue value,
                           DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //data is packed in the single value
            if(Bytes.compareTo(value.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            else if(Bytes.compareTo(PACKED_COLUMN_KEY,value.getQualifier())!=0)
                return; //don't try to unpack unless it's the right column

            try {
                byte[] data = Snappy.uncompress(value.getValue());
                unpack(fields, reversedKeyColumns, rowDecoder, data);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public boolean isColumnar() {
            return false;
        }
    };

    public static RowMarshall packed(){
        return PACKED;
    }

    public static RowMarshall packedCompressed(){
        return PACKED_COMPRESSED;
    }

    public static RowMarshall sparsePacked() {
        return SPARSE_PACKED;
    }
}
