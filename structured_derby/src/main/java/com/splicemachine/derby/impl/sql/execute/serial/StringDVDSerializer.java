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
    public void deserialize(byte[] bytes, DataValueDescriptor dvd,boolean desc) throws Exception {
        dvd.setValue(Encoding.decodeString(bytes,desc));
    }

    @Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return Encoding.encode(dvd.getString());
    }


}
