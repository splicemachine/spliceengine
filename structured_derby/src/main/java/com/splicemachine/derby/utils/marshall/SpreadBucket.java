package com.splicemachine.derby.utils.marshall;

import com.splicemachine.constants.bytes.BytesUtil;

/**
 * Bucketing logic for Spreading rows about TEMP, etc.
 * by using a fixed number of buckets.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public enum SpreadBucket {
		FOUR{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xC0);
				}

				@Override public int getNumBuckets() { return 4; }
				@Override public byte getMask() { return (byte)0xC0; }
		},
		EIGHT{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xE0);
				}

				@Override public int getNumBuckets() { return 8; }
				@Override public byte getMask() { return (byte)0xE0; }
		},
		SIXTEEN{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xF0);
				}

				@Override public int getNumBuckets() { return 16; }
				@Override public byte getMask() { return (byte)0xF0; }
		},
		THIRTY_TWO{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xF8);
				}

				@Override public int getNumBuckets() { return 32; }

				@Override public byte getMask() { return (byte)0xF8; }
		},
		SIXTY_FOUR{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xFC);
				}

				@Override public int getNumBuckets() { return 64; }

				@Override public byte getMask() { return (byte)0xFC; }
		},
		ONE_TWENTY_EIGHT{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xFE);
				}

				@Override public int getNumBuckets() { return 128; }

				@Override public byte getMask() { return (byte)0XFE; }
		},
		TWO_FIFTY_SIX{
				@Override
				public byte bucket(int hashValue) {
						return (byte)((byte)hashValue & 0xFF);
				}

				@Override public int getNumBuckets() { return 256; }
				@Override public byte getMask() { return (byte)0XFF; }
		};


		public byte bucket(int hashValue){
			throw new UnsupportedOperationException();
		}

		public int getNumBuckets(){
				throw new UnsupportedOperationException();
		}


		public byte getMask(){
				throw new UnsupportedOperationException();
		}
}
