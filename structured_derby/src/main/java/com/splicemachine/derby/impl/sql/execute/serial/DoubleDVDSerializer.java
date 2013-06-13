package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.types.DataValueDescriptor;


public class DoubleDVDSerializer implements DVDSerializer {

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception {
        ldvd.setValue(Encoding.decodeDouble(bytes));
    }

    @Override
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd) throws Exception {
        ldvd.setValue(Encoding.decodeDouble(bytes));
    }

    @Override
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd,boolean desc) throws Exception {
        ldvd.setValue(Encoding.decodeDouble(bytes,desc));
    }

    @Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return Encoding.encode(obj.getDouble());
    }

    @Override
    public ByteBuffer serialize(DataValueDescriptor obj, boolean desc) throws Exception {
        return ByteBuffer.wrap(Encoding.encode(obj.getDouble(),desc));
    }
}
