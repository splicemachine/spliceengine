package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.nio.ByteBuffer;

public class StringDVDSerializer implements DVDSerializer {

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd) throws Exception {
        dvd.setValue(Encoding.decodeString(bytes));
    }

    @Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return Encoding.encode(dvd.getString());
    }

    @Override
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd) throws Exception {
        ldvd.setValue(Encoding.decodeString(bytes));
    }

    @Override
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd,boolean desc) throws Exception {
        ldvd.setValue(Encoding.decodeString(bytes,desc));
    }

    @Override
    public ByteBuffer serialize(DataValueDescriptor obj, boolean desc) throws Exception {
        return ByteBuffer.wrap(Encoding.encode(obj.getString(),desc));
    }
}
