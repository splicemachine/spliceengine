package com.splicemachine.derby.impl.sql.execute.serial;

import com.gotometrics.orderly.BigDecimalRowKey;
import com.gotometrics.orderly.DoubleRowKey;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.math.BigDecimal;

public class DoubleDVDSerializer implements DVDSerializer {

    private BigDecimalRowKey rowKey = new BigDecimalRowKey();

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor ldvd) throws Exception {
        BigDecimal bd = (BigDecimal) rowKey.deserialize(bytes);
        ldvd.setValue(bd.doubleValue());
    }

    @Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return rowKey.serialize(new BigDecimal(obj.getDouble()));
    }
}
