package com.splicemachine.testutil;

/**
 *
 */
public class BitFormat {

    /**
     * Format the specified integer as binary string.
     */
    public static String pad(int number) {
        String num = Integer.toBinaryString(number);
        char[] zeros = new char[Integer.numberOfLeadingZeros(number)];
        for (int i = 0; i < zeros.length; i++) {
            zeros[i] = '0';
        }
        return new String(zeros) + num;
    }

    /**
     * Format the specified long as binary string.
     */
    public static String pad(long number) {
        String num = Long.toBinaryString(number);
        char[] zeros = new char[Long.numberOfLeadingZeros(number)];
        for (int i = 0; i < zeros.length; i++) {
            zeros[i] = '0';
        }
        return new String(zeros) + num;
    }
}
