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
				return formatLong(timestamp);
		}

		@Override
		protected Timestamp toTimestamp(long time) {
				return parseTimestamp(time);
		}


		public static long formatLong(Timestamp timestamp){
				long millis = timestamp.getTime();
				long micros = timestamp.getNanos()/NANOS_TO_MICROS;

				return millis*MICROS_TO_SECOND + micros;
		}

		public static Timestamp parseTimestamp(long time) {
				int micros = (int)(time % MICROS_TO_SECOND);
				long millis;
				if(micros<0){
						micros = MICROS_TO_SECOND +micros;
						time -=micros;
				}

				millis = time/ MICROS_TO_SECOND;

				Timestamp ts = new Timestamp(millis);
				ts.setNanos(micros*NANOS_TO_MICROS);
				return ts;
		}
}
