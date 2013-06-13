package com.splicemachine.derby.impl.sql.execute.serial;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.nio.ByteBuffer;

public interface DVDSerializer {

    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception;
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd) throws Exception;
    public void deserialize(ByteBuffer bytes, DataValueDescriptor ldvd,boolean desc) throws Exception;
    public byte[] serialize(DataValueDescriptor obj) throws Exception;
    public ByteBuffer serialize(DataValueDescriptor obj,boolean desc) throws Exception;
}
