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

import com.splicemachine.db.iapi.reference.SQLState;

import java.sql.SQLException;
import java.time.format.DateTimeFormatterBuilder;

import static java.lang.String.format;

public class SpliceDateFormatter {

    private String format = null;
    private java.time.format.DateTimeFormatter formatter = null;

    public SpliceDateFormatter(String format) {
        this.format = format != null ? format : "yyyy-M-d";
        
        if (format != null) {
            // The valid format characters of DateTimeFormatter documented here
            // can be used to identify separator characters like '-' or '/' :
            // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

            // But technically any non-format character can be a separator character.
            // This pattern matches one or more non-format characters or the empty string.
            String excludeFormatCharsPattern = "((?![\\s\\S])|([^GuyDMLdQqYwWEecFahKkHmsSAnNVzOXxZp]+))";

            // The string to replace ("MM" or "dd") may occur at the beginning or
            // end of the format string, with no characters preceding it
            // or following it.

            // We must not replace "MM" or "dd" if it's neighbor is a
            // non-format character. So, let's construct a pattern which matches
            // either one or more non-format characters or the empty string.
            // This pattern must be present on both sides of the match string.
            //
            // References:
            // https://stackoverflow.com/questions/19127384/what-is-a-regex-to-match-only-an-empty-string

            // Replace MM with M in the date format string.
            String matchPattern =
                    format("%s([M])([M])%s", excludeFormatCharsPattern,
                                             excludeFormatCharsPattern);
            this.format = this.format.replaceFirst(matchPattern, "$1$2$4");

            // Replace dd with d in the date format string.
            matchPattern =
                    format("%s([d])([d])%s", excludeFormatCharsPattern,
                                             excludeFormatCharsPattern);
            this.format = this.format.replaceFirst(matchPattern, "$1$2$4");
        }
        try {
            this.formatter =
                java.time.format.DateTimeFormatter.ofPattern(this.format);
        }
        catch (IllegalArgumentException e) {
            // If the format is bad, this.formatter will be null, and the next time
            // getFormatter() is called we'll throw a SQLException.
        }
    }
    public String getFormat() {
        return format;
    }
    public java.time.format.DateTimeFormatter getFormatter() throws SQLException {
        if (formatter == null)
            throw new SQLException("Error parsing datetime with pattern: "+this.format+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd", SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        return formatter;
    }
    public void setFormatter(java.time.format.DateTimeFormatter newFormatter) {formatter = newFormatter;}
}