package com.splicemachine.derby.utils.marshall;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public class SaltedKeyMarshall implements KeyMarshall{
    private Snowflake.Generator generator;

    public SaltedKeyMarshall() {
        this.generator = SpliceDriver.driver().getUUIDGenerator().newGenerator(SpliceConstants.maxImportReadBufferSize);
    }

    public SaltedKeyMarshall(Snowflake.Generator generator) {
        this.generator = generator;
    }

    @Override
    public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
        /*
         * It is safe to setRawBytes directly here, since the row key contains no actual information,
         * and thus can't be decoded anyway.
         */
        keyEncoder.setRawBytes(generator.nextBytes());
    }

    @Override
    public void decode(DataValueDescriptor[] data, int[] reversedKeyColumns, boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
        //no-op, no fields are present in the key
    }

    @Override
    public int getFieldCount(int[] keyColumns) {
        return 1;
    }

}
