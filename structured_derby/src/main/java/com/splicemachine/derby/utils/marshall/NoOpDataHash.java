package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * Represents a DataHash which does not encode or decode anything.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class NoOpDataHash implements DataHash,KeyHashDecoder{
		public static final DataHash INSTANCE = new NoOpDataHash();

		private NoOpDataHash(){}

		@SuppressWarnings("unchecked")
		public static <T> DataHash<T> instance(){
				return (DataHash<T>)INSTANCE;
		}
		@Override
		public void setRow(Object rowToEncode) {
				//no-op
		}

		@Override
		public byte[] encode() throws StandardException {
				return new byte[0];
		}

		@Override
		public KeyHashDecoder getDecoder() {
				return this;
		}

		@Override
		public void set(byte[] bytes, int hashOffset,int length) {
				//no-op
		}

		@Override
		public void decode(ExecRow destination) throws StandardException {
				//no-op
		}
}
