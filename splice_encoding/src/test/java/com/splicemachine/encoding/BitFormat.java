/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
