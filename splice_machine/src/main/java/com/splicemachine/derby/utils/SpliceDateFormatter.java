/*
 * Copyright (c) 2018 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import static java.lang.String.format;

public class SpliceDateFormatter {

    private String format = null;
    private java.time.format.DateTimeFormatter formatter;

    public SpliceDateFormatter(String format) {
        this.format = format != null ? format : "yyyy-M-d";
        
        if (format != null) {
            // The valid format characters of DateTimeFormatter:
            // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
            // This pattern excludes matching of format characters
            // before or after the match string.
            String excludeFormatCharsPattern1 = "([^GuyDMLdQqYwWEecFahKkHmsSAnNVzOXxZp]+)";
            String excludeFormatCharsPattern2 = "([^GuyDMLdQqYwWEecFahKkHmsSAnNVzOXxZp]*)";

            // The match string of MM or DD may occur at the beginning or
            // end of the format string, with no characters preceding it
            // or following it.  So perform the matched replacement twice,
            // where we match zero or more format characters on one side
            // of the match string and one or more format characters
            // on the other side of the match string.

            // Replace MM with M in the date format string.
            String pattern1 =
                    format("%s([M])([M])%s", excludeFormatCharsPattern1,
                                             excludeFormatCharsPattern2);
            String pattern2 =
                    format("%s([M])([M])%s", excludeFormatCharsPattern2,
                                             excludeFormatCharsPattern1);

            int stringLength = this.format.length();
            this.format = this.format.replaceFirst(pattern1, "$1$2$4");
            if (stringLength == this.format.length()) {
                this.format = this.format.replaceFirst(pattern2, "$1$2$4");
            }
            else
                stringLength = this.format.length();
    
            // Replace dd with d in the date format string.
            pattern1 = format("%s([d])([d])%s", excludeFormatCharsPattern1,
                                                excludeFormatCharsPattern2);
            pattern2 = format("%s([d])([d])%s", excludeFormatCharsPattern2,
                                                excludeFormatCharsPattern1);
            this.format = this.format.replaceFirst(pattern1, "$1$2$4");
            if (stringLength == this.format.length()) {
                this.format = this.format.replaceFirst(pattern2, "$1$2$4");
            }
        }
        this.formatter =
              java.time.format.DateTimeFormatter.ofPattern(this.format);
    }
    public String getFormat() {
        return format;
    }
    public java.time.format.DateTimeFormatter getFormatter() { return formatter; }
    public void setFormatter(java.time.format.DateTimeFormatter newFormatter) {formatter = newFormatter;}
}