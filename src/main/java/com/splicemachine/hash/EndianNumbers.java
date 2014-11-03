package com.splicemachine.hash;

/**
 * @author Scott Fines
 *         Date: 8/6/14
 */
 class EndianNumbers {
    public static void toBigEndianBytes(int i, byte[] destination, int pos){
        destination[pos  ] = (byte)(i>>24);
        destination[pos+1] = (byte)(i>>16);
        destination[pos+2] = (byte)(i>> 8);
        destination[pos+3] = (byte)(i    );
    }

    public static void toBigEndianBytes(long l,byte[] bytes,int pos){
        bytes[pos] =   (byte)(l>>56);
        bytes[pos+1] = (byte)(l>>48);
        bytes[pos+2] = (byte)(l>>40);
        bytes[pos+3] = (byte)(l>>32);
        bytes[pos+4] = (byte)(l>>24);
        bytes[pos+5] = (byte)(l>>16);
        bytes[pos+6] = (byte)(l>> 8);
        bytes[pos+7] = (byte)(l    );
    }

    public static byte[] toLittleEndianBytes(long l){
        byte[] bytes = new byte[8];
        bytes[7] = (byte)((l & 0xff00000000000000l) >>>56);
        bytes[6] = (byte)((l & 0x00ff000000000000l) >>>48);
        bytes[5] = (byte)((l & 0x0000ff0000000000l) >>>40);
        bytes[4] = (byte)((l & 0x000000ff00000000l) >>>32);
        bytes[3] = (byte)((l & 0x00000000ff000000l) >>>24);
        bytes[2] = (byte)((l & 0x0000000000ff0000l) >>>16);
        bytes[1] = (byte)((l & 0x000000000000ff00l) >>>8);
        bytes[0] = (byte)((l & 0x00000000000000ffl));
        return bytes;
    }


    public static long littleEndianLong(byte[] data, int pos){
        byte b1 = data[pos];
        byte b2 = data[pos+1];
        byte b3 = data[pos+2];
        byte b4 = data[pos+3];
        byte b5 = data[pos+4];
        byte b6 = data[pos+5];
        byte b7 = data[pos+6];
        byte b8 = data[pos+7];

        return  (((long)b8       )<<56) |
                (((long)b7 & 0xff)<<48) |
                (((long)b6 & 0xff)<<40) |
                (((long)b5 & 0xff)<<32) |
                (((long)b4 & 0xff)<<24) |
                (((long)b3 & 0xff)<<16) |
                (((long)b2 & 0xff)<<8) |
                (((long)b1 & 0xff));
    }

    public static int littleEndianInt(char[] bytes, int offset) {
        char b0 = bytes[offset];
        char b1 = bytes[offset+1];
        char b2 = bytes[offset+2];
        char b3 = bytes[offset+3];
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    public static long littleEndianLong(char[] bytes, int offset) {
        char b1 = bytes[offset];
        char b2 = bytes[offset+1];
        char b3 = bytes[offset+2];
        char b4 = bytes[offset+3];
        char b5 = bytes[offset+4];
        char b6 = bytes[offset+5];
        char b7 = bytes[offset+6];
        char b8 = bytes[offset+7];
        return  (((long)b8       )<<56) |
                (((long)b7 & 0xff)<<48) |
                (((long)b6 & 0xff)<<40) |
                (((long)b5 & 0xff)<<32) |
                (((long)b4 & 0xff)<<24) |
                (((long)b3 & 0xff)<<16) |
                (((long)b2 & 0xff)<<8) |
                (((long)b1 & 0xff));
    }

    public static int littleEndianInt(byte[] bytes, int offset) {
        byte b0 = bytes[offset];
        byte b1 = bytes[offset+1];
        byte b2 = bytes[offset+2];
        byte b3 = bytes[offset+3];
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    public static int littleEndianInt(CharSequence bytes, int offset) {
        char b0 = bytes.charAt(offset);
        char b1 = bytes.charAt(offset+1);
        char b2 = bytes.charAt(offset+2);
        char b3 = bytes.charAt(offset+3);
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }
}
