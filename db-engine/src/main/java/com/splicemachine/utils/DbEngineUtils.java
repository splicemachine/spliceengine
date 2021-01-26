package com.splicemachine.utils;

public class DbEngineUtils {

    public static String getJavaRegexpFilterFromAsteriskFilter(String asteriskFilter)
    {
        String filter = asteriskFilter;
        filter = filter.replace("\\", "\\\\");
        String toEscape[] = {"<", "(", "[", "{", "^", "-", "=", "$", "!", "|", "]", "}", ")", "+", ".", ">"};
        for(String s : toEscape) {
            filter = filter.replace(s, "\\" + s);
        }
        filter = filter.replace("*", ".*");
        return filter.replace("?", ".");
    }
}
