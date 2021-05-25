/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.db.shared.common.sql;

import java.util.List;
import java.util.stream.Collectors;
import java.nio.charset.Charset;

public class Utils {
    public final static char defaultEscapeCharacter = '\\';
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

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
    /**
     * this also checks if the format is a valid fixed-size timestamp format
     * see also https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
     * we support:
     * yyyy, yy, uuuu, uu, eee, EEE, MM, mm, dd, HH, hh, ss, a
     * up to 9x S (nanoseconds), and other characters " ,.-/:"
     * @param format
     * @throws IllegalArgumentException if format is not supported
     * @return length of format
     */
    public static int getTimestampFormatLength(String format) throws IllegalArgumentException
    {
        byte[] b = format.getBytes(UTF8_CHARSET );
        int repeat = 0;
        char last = (char)b[0];
        int length = b.length;
        for(int i=0; i<b.length+1; i++) {
            if (i != b.length && (char) b[i] == last) {
                repeat++;
                continue;
            }
            int r = repeat;
            char l = last;
            repeat = 1;
            if( i != b.length)
                last = (char) b[i];
            if ((l == 'Y' || l == 'y' || l == 'u') && (r == 4 || r == 2)) continue;
            else if (r == 2 &&  "MmdHhs".indexOf(l) != -1 ) continue;
            else if (r == 3 && (l == 'e' || l == 'E')) continue;
            else if ( (l == 'q' || l == 'Q') && (r == 1 || r == 2)) continue;
            else if (l == 'S' && r <= 9) continue; // s fraction max 9
            else if (r == 1 && l == 'a') {
                length++;
                continue;
            }
            else if(" ,.-/:".indexOf(l) != -1) continue;
            throw new IllegalArgumentException("not supported format \"" + format + "\": '" + l + "' can't be repeated " + r + " times");
        }
        return length;
    }
}
