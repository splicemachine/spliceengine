package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;

public interface DVDSerializer {

    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception;
    public byte[] serialize(DataValueDescriptor obj) throws Exception;
}
