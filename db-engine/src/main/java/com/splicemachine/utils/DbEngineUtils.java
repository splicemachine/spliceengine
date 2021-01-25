package com.splicemachine.utils;

public class DbEngineUtils {

    public static String getJavaRegexpFilterFromAsterixFilter(String asterixFilter)
    {
        String filter = asterixFilter;
        String toEscape[] = {"<", "(", "[", "{", "^", "-", "=", "$", "!", "|", "]", "}", ")", "+", ".", ">"};
        for(String s : toEscape) {
            filter = filter.replaceAll("\\" + s, "\\\\" + s);
        }
        filter.replaceAll("\\\\", "\\\\\\\\");

        filter = filter.replaceAll("\\*", ".*");
        return filter.replaceAll("\\?", ".");
    }
}
