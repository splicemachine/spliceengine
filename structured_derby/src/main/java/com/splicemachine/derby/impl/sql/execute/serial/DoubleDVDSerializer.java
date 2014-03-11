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

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode(obj.getDouble(),desc);
		}

		@Override
    public void deserialize(byte[] bytes, DataValueDescriptor ldvd, boolean desc) throws Exception {
        ldvd.setValue(Encoding.decodeDouble(bytes,desc));
    }

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setValue(Encoding.decodeDouble(bytes,offset,desc));
		}
}
