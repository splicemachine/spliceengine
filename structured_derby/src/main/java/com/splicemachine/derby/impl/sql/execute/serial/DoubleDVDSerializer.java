package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.types.DataValueDescriptor;


public class DoubleDVDSerializer implements DVDSerializer {

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception {
        ldvd.setValue(Encoding.decodeDouble(bytes));
    }

    @Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return Encoding.encode(obj.getDouble());
    }
}
