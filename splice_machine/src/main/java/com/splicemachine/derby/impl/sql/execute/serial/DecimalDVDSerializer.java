package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.math.BigDecimal;

public class DecimalDVDSerializer implements DVDSerializer {

		@Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return Encoding.encode( (BigDecimal) obj.getObject());
    }

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode( (BigDecimal) obj.getObject(),desc);
		}

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setBigDecimal(Encoding.decodeBigDecimal(bytes,offset,length,desc));
		}
}
