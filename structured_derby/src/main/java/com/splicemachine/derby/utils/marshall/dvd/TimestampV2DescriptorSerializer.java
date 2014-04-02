package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class TimestampV2DescriptorSerializer extends TimestampV1DescriptorSerializer {
		public static final Factory INSTANCE_FACTORY = new AbstractTimeDescriptorSerializer.Factory() {
				@Override public DescriptorSerializer newInstance() { return new TimestampV2DescriptorSerializer(); }
				@Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_TIMESTAMP_ID; }
		};

		private static final int MICROS_TO_SECOND = 1000000;
		private static final int NANOS_TO_MICROS = 1000;

		@Override
		protected long toLong(Timestamp timestamp) throws StandardException {
				long millis = timestamp.getTime();
				long micros = timestamp.getNanos()/NANOS_TO_MICROS;

				return millis*MICROS_TO_SECOND + micros;
		}

		@Override
		protected Timestamp toTimestamp(long time) {
				int micros = (int)(time % MICROS_TO_SECOND);
				long millis;
				if(time<0){
						micros = MICROS_TO_SECOND -micros;
						millis = (time - micros)/ MICROS_TO_SECOND;
				}else
						millis = time/ MICROS_TO_SECOND;

				Timestamp ts = new Timestamp(millis);
				ts.setNanos(micros*NANOS_TO_MICROS);
				return ts;
		}
}
