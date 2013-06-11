package com.splicemachine.derby.impl.sql.execute.serial;

import org.apache.derby.iapi.types.DataValueDescriptor;

public interface DVDSerializer {

    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception;
    public byte[] serialize(DataValueDescriptor obj) throws Exception;
}
