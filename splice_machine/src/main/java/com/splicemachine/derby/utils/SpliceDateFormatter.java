/*
 * Copyright (c) 2018-2019 Splice Machine, Inc.
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
import java.time.format.DateTimeFormatter;
import static java.lang.String.format;

public class SpliceDateFormatter {

    private final String format;
    private DateTimeFormatter formatter = null;

/**
 * Splice formatter for parsing date strings for the TO_DATE function
 * and IMPORT statements.
 * <p>
 * This class provides the main application entry point for parsing
 * of dates with a specified format such as {@code yyyy-MM-dd}.
 * <p>
 * See https://doc.splicemachine.com/sqlref_builtinfcns_todate.html for
 * information on supported Splice Machine date/timestamp format strings.
 */
    public SpliceDateFormatter(String format) {

        // The default external format of "yyyy-MM-dd" may be used
        // in error messages, but the corresponding internally used format
        // is "yyyy-M-d".
        if (format == null) {
            format = "yyyy-M-d";
            this.format = "yyyy-MM-dd";
        }
        else {
            this.format = format;

            // The valid format characters of DateTimeFormatter documented at the following link
            // can be used to identify separator characters like '-' or '/' :
            // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

            // Any non-format character can be a separator character.
            // This pattern matches one or more non-format characters.
            String
            excludeFormatCharsPattern = "([^GuyDMLdQqYwWEecFahKkHmsSAnNVzOXxZp]+)";

            // The string to replace ("MM" or "dd") may occur at the beginning or
            // end of the format string, with no characters preceding it
            // or following it.

            // We must not replace "MM" or "dd" if its neighbor is a
            // format character.
            // This pattern excludeFormatCharsPattern or the empty string
            // must be present on both sides of the match string.

            // Replace MM with M in the date format string.
            String matchPattern;

            matchPattern = format("(^[M])([M])%s", excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3");
            matchPattern = format("%s([M])([M]$)", excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3");
            matchPattern = format("%s([M])([M])%s", excludeFormatCharsPattern,
                                                    excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3$4");

            // Replace dd with d in the date format string.
            matchPattern = format("(^[d])([d])%s", excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3");
            matchPattern = format("%s([d])([d]$)", excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3");
            matchPattern = format("%s([d])([d])%s", excludeFormatCharsPattern,
                                                    excludeFormatCharsPattern);
            format = format.replaceFirst(matchPattern, "$1$3$4");
        }
        try {
            this.formatter =
                DateTimeFormatter.ofPattern(format);
        }
        catch (IllegalArgumentException e) {
            // If the format is bad, this.formatter will be null, and the next time
            // getFormatter() is called we'll throw a SQLException.
        }
    }

    /**
     * Returns the original date format used in the SQL (such as in a TO_DATE function).
     * This may differ from the internal format used inside this class wh
     *
     * @param  none
     * @return The date format string, such as {@code yyyy-MM-dd}
     * @notes The returned format may differ from the internal format used inside
     *        this class.  This class utilizes the java.time.format.DateTimeFormatter
     *        class internally, but with a different internal format necessary
     *        to match the functionality documented for Splice date formats:
     *        https://doc.splicemachine.com/sqlref_builtinfcns_todate.html
     */
    public String getFormat() {
        return format;
    }

    public DateTimeFormatter getFormatter() throws SQLException {
        if (formatter == null)
            throw new SQLException("Error parsing datetime with pattern: "+this.format+". Try using an" +
                " ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd",
                SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        return formatter;
    }
    
    public void setFormatter(DateTimeFormatter newFormatter) {formatter = newFormatter;}
}