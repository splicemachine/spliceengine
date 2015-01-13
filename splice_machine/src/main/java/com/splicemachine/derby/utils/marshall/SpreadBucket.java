package com.splicemachine.derby.utils.marshall;

/**
 * Bucketing logic for Spreading rows about TEMP, etc. by using a fixed number of buckets.
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
                @Override public int bucketIndex(byte b) { return ((byte)(b >> 3) & 0x1f); }
        },
        SIXTY_FOUR{
                @Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFC); }
                @Override public int getNumBuckets() { return 64; }
                @Override public byte getMask() { return (byte)0xFC; }
                @Override public int bucketIndex(byte b) { return ((byte)(b >> 2) & 0x3f); }
        },
        ONE_TWENTY_EIGHT{
                @Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFE); }
                @Override public int getNumBuckets() { return 128; }
                @Override public byte getMask() { return (byte)0XFE; }
                @Override public int bucketIndex(byte b) { return ((byte)(b >> 1) & 0x7f); }
        },
        TWO_FIFTY_SIX{
                @Override public byte bucket(int hashValue) { return (byte)((byte)hashValue & 0xFF); }
                @Override public int getNumBuckets() { return 256; }
                @Override public byte getMask() { return (byte)0XFF; }
        };

        public abstract byte bucket(int hashValue);

        public abstract int getNumBuckets();

        public abstract byte getMask();

        public int bucketIndex(byte b) {
            /* Apparently we only use bucket sixteen currently. */
            throw new UnsupportedOperationException();
        }
        
        public static SpreadBucket getValue(int numBuckets) {
        	// We don't yet have support for all these variations. They are coming soon,
        	// but until then we guard against invalid choice.
        	switch (numBuckets) {
        	// case 4: return FOUR;
        	// case 8: return EIGHT;
        	case 16: return SIXTEEN;
        	case 32: return THIRTY_TWO;
        	case 64: return SIXTY_FOUR;
        	case 128: return ONE_TWENTY_EIGHT;
        	// case 256: return TWO_FIFTY_SIX;
        	default: return SIXTEEN;
        	}
        }

}
