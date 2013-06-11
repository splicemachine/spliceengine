package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.types.DataValueDescriptor;

public class StringDVDSerializer implements DVDSerializer {

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd) throws Exception {
        dvd.setValue(Encoding.decodeString(bytes));
    }

    @Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return Encoding.encodeNullFree(dvd.getString());
    }


}
