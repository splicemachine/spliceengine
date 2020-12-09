package com.splicemachine.db.shared.common.sql;

public class Utils {
    public final static char defaultEscapeCharacter = '\\';

    public static String escape(String in) {
        return escape(in, defaultEscapeCharacter);
    }

    public static String escape(String in, char escapeCharacter) {
        if(in == null) {
            return null;
        }
        return in.replace(Character.toString(escapeCharacter), Character.toString(escapeCharacter) + escapeCharacter)
                .replace("_", escapeCharacter + "_")
                .replace("%", escapeCharacter + "%");
    }
}