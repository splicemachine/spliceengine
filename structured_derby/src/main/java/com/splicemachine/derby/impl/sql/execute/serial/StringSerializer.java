package com.splicemachine.derby.impl.sql.execute.serial;

import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.UTF8RowKey;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class StringSerializer implements SerializerThunk {

    private RowKey rowKey = new UTF8RowKey();

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd) throws Exception {
        dvd.setValue((String) rowKey.deserialize(bytes));
    }

    @Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return rowKey.serialize(dvd.getString());
    }


}
