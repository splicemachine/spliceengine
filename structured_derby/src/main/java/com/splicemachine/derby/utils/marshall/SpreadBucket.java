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
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xC0); }
				@Override public int getNumBuckets() { return 4; }
				@Override public byte getMask() { return (byte)0xC0; }
		},
		EIGHT{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xE0); }
				@Override public int getNumBuckets() { return 8; }
				@Override public byte getMask() { return (byte)0xE0; }
		},
		SIXTEEN{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xF0); }
				@Override public int getNumBuckets() { return 16; }
				@Override public byte getMask() { return (byte)0xF0; }
        @Override public int bucketIndex(byte b) { return ((byte)(b >> 4) & 0x0f); }
    },
		THIRTY_TWO{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xF8); }
				@Override public int getNumBuckets() { return 32; }
				@Override public byte getMask() { return (byte)0xF8; }
		},
		SIXTY_FOUR{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFC); }
				@Override public int getNumBuckets() { return 64; }
				@Override public byte getMask() { return (byte)0xFC; }
		},
		ONE_TWENTY_EIGHT{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFE); }
				@Override public int getNumBuckets() { return 128; }
				@Override public byte getMask() { return (byte)0XFE; }
		},
		TWO_FIFTY_SIX{
				@Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFF); }
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

    public int bucketIndex(byte b) {
        throw new UnsupportedOperationException();
    }

    public static void main(String...args) throws Exception{
        SpreadBucket bucket = SpreadBucket.SIXTEEN;
        System.out.println(bucket.bucketIndex((byte)-128));
        System.out.println(bucket.bucketIndex((byte) 0x00));
        System.out.println(bucket.bucketIndex((byte) 0x10));
        System.out.println(bucket.bucketIndex((byte) 0x20));
        System.out.println(bucket.bucketIndex((byte) 0x30));
        System.out.println(bucket.bucketIndex((byte) 0x40));
        System.out.println(bucket.bucketIndex((byte) 0x50));
        System.out.println(bucket.bucketIndex((byte) 0x60));
        System.out.println(bucket.bucketIndex((byte) 0x70));
        System.out.println(bucket.bucketIndex((byte) 0x80));
        System.out.println(bucket.bucketIndex((byte) 0x90));
        System.out.println(bucket.bucketIndex((byte) 0xA0));
        System.out.println(bucket.bucketIndex((byte) 0xB0));
        System.out.println(bucket.bucketIndex((byte) 0xC0));
        System.out.println(bucket.bucketIndex((byte) 0xD0));
        System.out.println(bucket.bucketIndex((byte) 0xE0));
        System.out.println(bucket.bucketIndex((byte) 0xF0));
    }

}
