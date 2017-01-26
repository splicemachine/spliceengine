/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.encoding;

import org.apache.commons.lang.StringUtils;

/**
 * Format a byte/byte[]/short/integer/long as a formatted string of '1' and '0' characters consistent with its internal
 * java binary representation.
 * <p/>
 * NOTE: Integer.toBinaryString() can do this as well, but does not include the padding, spacing, etc.
 */
public class BitFormat {

    private boolean includeDecimal;

    public BitFormat() {
        this(true);
    }

    public BitFormat(boolean includeDecimal) {
        this.includeDecimal = includeDecimal;
    }

    public static String format(long inLong, int bits, boolean includeDecimal) {
        StringBuilder buffer = new StringBuilder(80);
        if (includeDecimal) {
            String left = StringUtils.rightPad(inLong + "", 11, ' ');
            buffer.append(left);
            buffer.append(" = ");
        }
        for (long i = (bits - 1); i >= 0; i--) {

            // Simple
            if (((1L << i) & inLong) != 0) {
                buffer.append('1');
            } else {
                buffer.append('0');
            }
            /* Divide into octets */
            if (i % 8 == 0 && i > 0) {
                buffer.append(' ');
            }
        }
        return buffer.toString();
    }

    public String format(byte[] byteArray) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < byteArray.length; i++) {
            builder.append(format(byteArray[i], 8, false));
            if (i != byteArray.length - 1) {
                builder.append(" ");
            }
        }
        return builder.toString();
    }

    public String format(byte inByte) {
        return format(inByte, 8, includeDecimal);
    }

    public String format(short inShort) {
        return format(inShort, 16, includeDecimal);
    }

    public String format(int inInt) {
        return format(inInt, 32, includeDecimal);
    }

    public String format(long inLong) {
        return format(inLong, 64, includeDecimal);
    }

}
