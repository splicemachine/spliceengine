package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/26/14
 * Time: 9:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeyDataHash extends SuppliedDataHash {

    private int[] keyColumns;
    private DataValueDescriptor[] kdvds;

    public KeyDataHash(StandardSupplier<byte[]> supplier, int[] keyColumns, DataValueDescriptor[] kdvds) {
        super(supplier);
        this.keyColumns = keyColumns;
        this.kdvds = kdvds;
    }

    @Override
    public KeyHashDecoder getDecoder() {
        return new SuppliedKeyHashDecoder(keyColumns, kdvds);
    }

    private class SuppliedKeyHashDecoder implements KeyHashDecoder {

        private int[] keyColumns;
        private DataValueDescriptor[] kdvds;
        MultiFieldDecoder decoder;

        public SuppliedKeyHashDecoder(int[] keyColumns, DataValueDescriptor[] kdvds) {
            this.keyColumns = keyColumns;
            this.kdvds = kdvds;
        }

        @Override
        public void set(byte[] bytes, int hashOffset, int length){
            if (decoder == null)
                decoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
            decoder.set(bytes, hashOffset, length);
        }

        @Override
        public void decode(ExecRow destination) throws StandardException {
            unpack(decoder, destination);
        }

        private void unpack(MultiFieldDecoder decoder, ExecRow destination) throws StandardException {
            DataValueDescriptor[] fields = destination.getRowArray();
            for (int i = 0; i < keyColumns.length; ++i) {
                if (keyColumns[i] == -1) {
                    // skip the key columns that are not in the result
                    DerbyBytesUtil.skip(decoder, kdvds[i]);
                }
                else {
                    DataValueDescriptor field = fields[keyColumns[i]];
                    decodeNext(decoder, field);
                }
            }
        }
        void decodeNext(MultiFieldDecoder decoder, DataValueDescriptor field) throws StandardException {
            if(DerbyBytesUtil.isNextFieldNull(decoder, field)){
                field.setToNull();
                DerbyBytesUtil.skip(decoder, field);
            }else
                DerbyBytesUtil.decodeInto(decoder,field);
        }
    }
}
