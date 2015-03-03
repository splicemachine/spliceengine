package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.db.iapi.types.DataValueDescriptor;


public class DoubleDVDSerializer implements DVDSerializer {


		@Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return Encoding.encode(obj.getDouble());
    }

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode(obj.getDouble(),desc);
		}

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setValue(Encoding.decodeDouble(bytes,offset,desc));
		}
}
