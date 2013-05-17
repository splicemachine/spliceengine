package com.splicemachine.derby.impl.sql.execute.serial;

import com.gotometrics.orderly.RowKey;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import org.apache.derby.iapi.types.DataValueDescriptor;

public class StringDVDSerializer implements DVDSerializer {

    private RowKey rowKey = new DerbyBytesUtil.NullRemovingRowKey();

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd) throws Exception {
        dvd.setValue((String) rowKey.deserialize(bytes));
    }

    @Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return rowKey.serialize(dvd.getString());
    }


}
