package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.joda.time.DateTime;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/12/14
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TimestampDVDSerializer implements DVDSerializer {

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd) throws Exception {
        dvd.setValue(new DateTime(Encoding.decodeLong(bytes)));
    }

    @Override
    public void deserialize(byte[] bytes, DataValueDescriptor dvd,boolean desc) throws Exception {
        dvd.setValue(new DateTime(Encoding.decodeLong(bytes, desc)));
    }

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setValue(new DateTime(Encoding.decodeLong(bytes,offset,desc)));
		}

		@Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return Encoding.encode(dvd.getDateTime().getMillis());
    }

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode(obj.getDateTime().getMillis(),desc);
		}
}
