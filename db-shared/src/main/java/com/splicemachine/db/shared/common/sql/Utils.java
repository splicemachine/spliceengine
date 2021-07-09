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

    public static String constructHbaseName(String tableName) {
        if(tableName == null) {
            return null;
        }
        try {
            long conglomId = Long.parseLong(tableName);
            return "splice:" + conglomId;
        } catch(NumberFormatException nfe) {
            if(!tableName.contains(":")) {
                throw new IllegalArgumentException(String.format("improper HBase table name %s", tableName));
            }
        }
        return tableName;
    }
}
