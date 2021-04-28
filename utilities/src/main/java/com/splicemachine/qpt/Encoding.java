package com.splicemachine.qpt;

public class Encoding {
    private static final String ENCODING = "0123456789#=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPRQSTUVWXYZ";
    private static final int ID_LENGTH = 8;

    /* ID is based on the hashCode; first byte matches the statement kind */
    public static String makeId(String prefix, long hash, int length) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = sb.length(); i < length; ++i) {
            sb.append(ENCODING.charAt((int)(hash & 0x3f)));
            hash >>= 6;
        }
        return sb.toString();
    }

    public static String makeId(String prefix, long hash) {
        return makeId(prefix, hash, ID_LENGTH);
    }
}
