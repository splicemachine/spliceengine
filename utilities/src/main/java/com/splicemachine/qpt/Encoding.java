/*
 * Copyright (c) 2021 Splice Machine, Inc.
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
