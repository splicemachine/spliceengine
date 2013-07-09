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

    private static KeyValue columnarConvert(DataValueDescriptor dvd, byte[] rowKey, int pos) throws StandardException {
        if(dvd!=null&&!dvd.isNull()){
            byte[] qual =Encoding.encode(pos);
            return new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,qual, DerbyBytesUtil.generateBytes(dvd));
        }
        return null;
    }

    private static final RowMarshall DENSE_COLUMNAR = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row, int[] rowColumns, Put put, MultiFieldEncoder rowEncoder) throws StandardException {
            List<KeyValue> kvs = Lists.newArrayListWithCapacity(row.length);
            encodeKeyValues(row,put.getRow(),rowColumns,null,kvs);
            for(KeyValue kv:kvs){
                try {
                    put.add(kv);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            boolean written = false;
            if(rowColumns==null){
                for(int i=0;i<row.length;i++){
                    KeyValue kv = convert(row[i],rowKey,i);
                    if(kv!=null){
                        written = true;
                        kvResults.add(kv);
                    }
                }
            }else{
                for(int rowPos:rowColumns){
                    KeyValue kv = convert(row[rowPos],rowKey,rowPos);
                    if(kv!=null){
                        written = true;
                        kvResults.add(kv);
                    }
                }
            }
            if(!written){
                kvResults.add(new KeyValue(rowKey, SpliceConstants.DEFAULT_FAMILY_BYTES,EMPTY_ROW_COLUMN,new byte[]{}));
            }
        }

        private KeyValue convert(DataValueDescriptor dvd, byte[] rowKey, int pos) throws StandardException{
            KeyValue kv = columnarConvert(dvd, rowKey, pos);
            if(kv==null&&dvd!=null)
                kv = new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(pos),new byte[]{});
            return kv;
        }

        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder) throws StandardException {
            COLUMNAR.decode(value,fields,reversedKeyColumns,rowDecoder);
        }

        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            COLUMNAR.fill(row,rowColumns,encoder);
        }

        @Override
        public boolean isColumnar() {
            return true;
        }
    };

    private static final RowMarshall MAPPED_COLUMNAR = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row, int[] rowColumns, Put put, MultiFieldEncoder rowEncoder) throws StandardException {
            COLUMNAR.encodeRow(row,rowColumns,put,rowEncoder);
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            COLUMNAR.encodeKeyValues(row,rowKey,rowColumns,null,kvResults);
        }

        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            COLUMNAR.fill(row,rowColumns,encoder);
        }

        @Override
        public void decode(KeyValue value,
                           DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //ignores rowDecoder, which is probably null anyway, and just picks it from the qualifier
            if(Bytes.compareTo(value.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            int pos = Encoding.decodeInt(value.getQualifier());
            if(pos<0) return; //skip negative columns

            byte[] data = value.getValue();
            if(reversedKeyColumns!=null&&reversedKeyColumns.length>0)
                pos = reversedKeyColumns[pos];
            DerbyBytesUtil.fromBytes(data,fields[pos]);
        }

        @Override
        public boolean isColumnar() {
            return true;
        }
    };

    private static final RowMarshall COLUMNAR = new RowMarshall() {
        @Override
        public void encodeRow(DataValueDescriptor[] row,
                              int[] rowColumns,
                              Put put,
                              MultiFieldEncoder rowEncoder) throws StandardException {
            List<KeyValue> kvs = Lists.newArrayListWithCapacity(row.length); //TODO -sf- can be smaller
            encodeKeyValues(row,put.getRow(),rowColumns,null,kvs);
            for(KeyValue kv:kvs){
                try {
                    put.add(kv);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }

        @Override
        public void encodeKeyValues(DataValueDescriptor[] row,
                                    byte[] rowKey,
                                    int[] rowColumns,
                                    MultiFieldEncoder rowEncoder,
                                    List<KeyValue> kvResults) throws StandardException {
            boolean written = false;
            if(rowColumns==null){
                for(int i=0;i<row.length;i++){
                    KeyValue kv = columnarConvert(row[i],rowKey,i);
                    if(kv!=null){
                        written = true;
                        kvResults.add(kv);
                    }
                }
            }else{
                for(int rowPos:rowColumns){
                    KeyValue kv = columnarConvert(row[rowPos],rowKey,rowPos);
                    if(kv!=null){
                        written = true;
                        kvResults.add(kv);
                    }
                }
            }
            if(!written){
                kvResults.add(new KeyValue(rowKey, SpliceConstants.DEFAULT_FAMILY_BYTES,EMPTY_ROW_COLUMN,new byte[]{}));
            }
        }


        @Override
        public void fill(DataValueDescriptor[] row, int[] rowColumns, MultiFieldEncoder encoder) throws StandardException {
            //no-op for COLUMNAR Types
        }

        @Override
        public void decode(KeyValue value, DataValueDescriptor[] fields, int[] reversedKeyColumns, MultiFieldDecoder rowDecoder) throws StandardException {
            //ignores rowDecoder, which is probably null anyway, and just picks it from the qualifier
            if(Bytes.compareTo(value.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            int pos = Encoding.decodeInt(value.getQualifier());
            if(pos<0) return; //skip negative columns

            byte[] data = value.getValue();
            DerbyBytesUtil.fromBytes(data,fields[pos]);
        }

        @Override
        public boolean isColumnar() {
            return true;
        }
    };

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
            for(int i=0;i<fields.length;i++){
                DataValueDescriptor dvd = fields[i];
                if(rowDecoder.nextIsNull()){
                    dvd.setToNull();
                    rowDecoder.skip();
                }else
                    DerbyBytesUtil.decodeInto(rowDecoder,dvd);
            }
        }
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

    public static RowMarshall denseColumnar(){
        return DENSE_COLUMNAR;
    }

    public static RowMarshall mappedColumnar(){
        return MAPPED_COLUMNAR;
    }

    public static RowMarshall columnar(){
        return COLUMNAR;
    }

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
