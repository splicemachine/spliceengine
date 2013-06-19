package com.splicemachine.derby.utils.marshall;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 6/12/13
 */
public enum RowType implements RowMarshall{

    /**
     * Each unkeyed field is given it's own column
     */
     DENSE_COLUMNAR {
        @Override
        public void encodeRow(DataValueDescriptor[] fields,
                              int[] rowColumns,
                              Put put,
                              MultiFieldEncoder rowEncoder) throws StandardException {
                /*
                 * rowEncoder is ignored, because we put each row's entry into a column based on
                 * its row position
                 */
            boolean written = false;
            for(int rowCol:rowColumns){
                DataValueDescriptor dvd = fields[rowCol];
                if(dvd!=null&&!dvd.isNull()){
                    byte[] data = DerbyBytesUtil.generateBytes(dvd);
                    put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(rowCol),data);
                    written=true;
                }else if(dvd!=null&&dvd.isNull()){
                    put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(rowCol),new byte[]{});
                }
            }
            if(!written){
                //no columns to store, so put one in place
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(-1000),new byte[]{});
            }
        }

        @Override
        public void decode(KeyValue value,
                           DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //ignores rowDecoder, which is probably null anyway, and just picks it from the qualifier
            if(Bytes.compareTo(value.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            int pos = Bytes.toInt(value.getQualifier());
            if(pos<0) return; //skip negative columns

            byte[] data = value.getValue();
            DerbyBytesUtil.fromBytes(data,fields[pos]);
        }
    },
    MAPPED_COLUMNAR {
        @Override
        public void encodeRow(DataValueDescriptor[] fields,
                              int[] rowColumns,
                              Put put,
                              MultiFieldEncoder rowEncoder) throws StandardException {
            COLUMNAR.encodeRow(fields,rowColumns,put,rowEncoder);
        }

        @Override
        public void decode(KeyValue value,
                           DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //ignores rowDecoder, which is probably null anyway, and just picks it from the qualifier
            if(Bytes.compareTo(value.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            int pos = Bytes.toInt(value.getQualifier());
            if(pos<0) return; //skip negative columns

            byte[] data = value.getValue();
            if(reversedKeyColumns!=null&&reversedKeyColumns.length>0)
                pos = reversedKeyColumns[pos];
            DerbyBytesUtil.fromBytes(data,fields[pos]);
        }
    },
    COLUMNAR {
        @Override
        public void encodeRow(DataValueDescriptor[] fields,
                              int[] rowColumns,
                              Put put,
                              MultiFieldEncoder rowEncoder) throws StandardException {
                /*
                 * rowEncoder is ignored, because we put each row's entry into a column based on
                 * its row position
                 */
            boolean written = false;
            for(int rowCol:rowColumns){
                DataValueDescriptor dvd = fields[rowCol];
                if(dvd!=null&&!dvd.isNull()){
                    byte[] data = DerbyBytesUtil.generateBytes(dvd);
                    put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(rowCol),data);
                    written=true;
                }
            }
            if(!written){
                //no columns to store, so put one in place
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(-1000),new byte[]{});
            }
        }

        @Override
        public void decode(KeyValue value,
                           DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //ignores rowDecoder, which is probably null anyway, and just picks it from the qualifier
            if(Bytes.compareTo(value.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)==0)
                return;
            int pos = Bytes.toInt(value.getQualifier());
            if(pos<0) return; //skip negative columns

            byte[] data = value.getValue();
            DerbyBytesUtil.fromBytes(data,fields[pos]);
        }
    },
    /**
     * All unkeyed fields are packed into a single byte[]
     */
    PACKED {

        @Override
        public void encodeRow(DataValueDescriptor[] fields,
                              int[] rowColumns,
                              Put put,
                              MultiFieldEncoder rowEncoder) throws StandardException {
                /*
                 * Encode the entire row into a single column in the put
                 * use the column value 0x00 as the column key
                 */
            for(int rowCol:rowColumns){
                DerbyBytesUtil.encodeInto(rowEncoder,fields[rowCol],false);
            }
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,rowEncoder.build());
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

            byte[] data = value.getValue();
            rowDecoder.set(data);
            for(int keyCol:reversedKeyColumns){
                DerbyBytesUtil.decodeInto(rowDecoder,fields[keyCol]);
            }
        }
    },
    /**
     * All unkeyed fields are packed into a single byte[], which is then compressed.
     */
    PACKED_COMPRESSED {
        @Override
        public void encodeRow(DataValueDescriptor[] fields,
                              int[] rowColumns,
                              Put put, MultiFieldEncoder rowEncoder) throws StandardException {
                /*
                 * Encode the entire row into a single column in the put
                 * use the column value 0x00 as the column key
                 */
            for(int rowCol:rowColumns){
                DerbyBytesUtil.encodeInto(rowEncoder,fields[rowCol],false);
            }

            byte[] data = rowEncoder.build();
            //compress the data
            try {
                byte[] compressed = Snappy.compress(data);
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,PACKED_COLUMN_KEY,compressed);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
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
                rowDecoder.set(data);
                for(int keyCol:reversedKeyColumns){
                    DerbyBytesUtil.decodeInto(rowDecoder, fields[keyCol]);
                }
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }
    };


    public static final byte[] PACKED_COLUMN_KEY = new byte[]{0x00};
}
