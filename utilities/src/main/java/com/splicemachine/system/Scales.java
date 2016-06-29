package com.splicemachine.system;

/**
 * @author Scott Fines
 *         Date: 1/13/15
 */
public class Scales {
    private Scales(){}

    public static final String[] FULL_BYTE_SCALES=new String[]{"bytes","kilobytes","megabytes","gigabytes","terabytes","petabytes","exabytes","zetabytes"};
    public static final String[] ABB_BYTE_SCALES=new String[]{"B","KB","MB","GB","TB","PB","EB","ZB"};

    /**
     * Scale the given number by the specified number of shifts. This make it easy to convert
     * from bytes to kilobytes, megabytes, etc.
     *
     * @param bytes the raw bytes to scale
     * @param shift the number of units to shift by
     * @return a scaled number of units
     */
    public static double scaleBytes(long bytes,int shift){
        if(shift==0) return (double)bytes;
        return bytes/(Math.pow(1024d,shift));
    }

    public static String abbByteLabel(int scale) {
        assert scale>=0 && scale< ABB_BYTE_SCALES.length: "Unknown scale: "+ scale;
        return ABB_BYTE_SCALES[scale];
    }

    public static String fullByteLabel(int scale) {
        assert scale>=0 && scale< FULL_BYTE_SCALES.length: "Unknown scale: "+ scale;
        return FULL_BYTE_SCALES[scale];
    }

    /**
     * @param bytes the raw bytes to scale
     * @return a String representing the bytes as scaled to the largest unit, along with an appropriate label
     */
    public static String printScaledBytes(long bytes){
        int s = 0;
        long l = bytes;
        while(l>1024){
            l>>=10;
            s++;
        }
        return scaleBytes(bytes,s)+" "+ abbByteLabel(s);
    }

}
