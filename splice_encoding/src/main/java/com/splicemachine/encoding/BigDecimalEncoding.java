/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Encapsulates logic for BigDecimal encoding.
 * <p/>
 * Serialize a BigDecimal to a byte[] in such a way as to preserve
 * the natural order of elements.
 * <p/>
 * The format for the serialization is as follows:
 * <p/>
 * **Summary**
 * The first byte is a header byte, similar to that used to encode longs.
 * The first 2 bits of the header byte are the signum information. The
 * remainder of that byte (plus additional bytes as necessary) will
 * be used to store the <em>adjusted scale</em> of the decimal
 * (described in a bit). The remaining bytes are used to store
 * the <em>Binary Encoded Decimal</em> format of the unscaled integer
 * form. This format uses 4 bits per decimal character, thus
 * requiring 1 byte for every 2 decimal characters in
 * the unscaled integer value(rounded up when
 * there are an odd number of decimal characters.)
 * <p/>
 * **Details**
 * BigDecimals are represented by a signed, arbitrary-precision
 * integer (called the <em>unscaled value</em>) and a 32-bit base-2
 * encoded <em>scale</em>. Thus, any big decimal can be represented
 * as {@code (-1)<sup>signum</sup>unscaledValue * 10<sup>-scale</sup>},
 * where <em>signum</em> is either -1,0, or 1 depending on whether
 * the decimal is negative, zero, or positive.
 * <p/>
 * Hence, there are three distinct elements which must be stored:
 * {@code signum, unscaled value, } and {@code scale}.
 * <p/>
 * To sort negatives before positives, we must first store the signum,
 * which requires a minimum of 2 bits. Because 0x00 is reserved for
 * combinatoric separators, we use the following mapping:
 * <p/>
 * -1   = 0x01
 * 0    = 0x02
 * 1    = 0x03
 * <p/>
 * After the signum, we store the <em>adjusted scale</em>. Essentially,
 * we are converting all the numbers to be stored into the same base,
 * and recording a new scale. Mathematically, a BigDecimal looks like
 * {@code (-1)<sup>signum</sup>*2<sup>m</sup>*10<sup>-scale</sup>}, where
 * {@code 2<sup>m</sup> } is the base-2 formatted unscaled value.
 * Converting everything into base-10, the unscaled value becomes
 * {@code b*10<sup>-p+1</sup>}, where {@code b} is a scalar and
 * {@code p} is the number of decimal digits in the unscaled
 * value. Thus, combining your exponents nets you
 * the base-10-formatted BigDecimal as
 * {@code (-1)<sup>signum</sup>*b*10<sup>p-scale-1</sup>}. Hence
 * the adjusted scale is {@code p+scale-1}.
 * <p/>
 * The adjusted scale requires up to 33 bits of storage if we
 * can use the same format as we use for serializing longs. So, we
 * serialize the adjusted scale using that same format but shifted
 * over by two bits to allow room for the signum
 * (see {@link com.splicemachine.encoding.ScalarEncoding.toBytes(long,boolean))} for more
 * information).
 * <p/>
 * Finally, we must serialize the base-10-formatted unscaled value
 * itself. Since that value is arbitrarily long, we use Binary
 * Encoded Decimals to serialize the character digits of the base-10
 * formatted scalar {@code b}.
 * <p/>
 * In Binary Encoded Decimals, each decimal character 0-9 is
 * transformed to a 4-bit int between 1-10 (e.g. add 1), and then
 * stored 2 characters to a byte. This means that, if there are {@code
 * N } decimal digits in the unscaled value, there will be {@code N/2}
 * bytes used to store them ({@code N/2 +1} if {@code N} is odd).
 * <p/>
 * Thus, the total storage required is 2 bits for the signum, between
 * 6 and 33-bits for the adjusted scale, and {@code N/2} ({@code N/2+1})
 * bytes for the unscaledValue, making the total space at least 1
 * byte.
 */
class BigDecimalEncoding {

    private static final int HEADER_SIZE_BITS = 2;

    private static final byte HEADER_NULL = 0x00;
    private static final byte HEADER_NEG = 0x01;
    private static final byte HEADER_ZERO = 0x02;
    private static final byte HEADER_POS = 0x03;

    private static final int ORDER_FLIP_MASK = 0xFF;
    private static final int ORDER_FLIP_EXCLUDE_HEADER_MASK = 0xFF >> HEADER_SIZE_BITS;

    public static byte[] toBytes(BigDecimal value, boolean desc) {
        byte[] ascendingBytes = toBytes(value);
        if (desc) {
            for (int i = 0; i < ascendingBytes.length; i++) {
                ascendingBytes[i] ^= ORDER_FLIP_MASK;
            }
        }
        return ascendingBytes;
    }

    public static byte[] toBytes(BigDecimal value) {
        //avoid having duplicate numerically equivalent representations
        value = value.stripTrailingZeros();
        BigInteger unscaled = value.unscaledValue();

        byte[] data;

        if (unscaled.signum() == 0) {
            data = new byte[1];
            byte b = HEADER_ZERO;
            data[0] = (byte) ((b << Byte.SIZE - HEADER_SIZE_BITS) & ORDER_FLIP_MASK);
            return data;
        }

        int precision = value.precision();
        long exp = precision - value.scale() - 1;
        /*
         * We need to serialize the exponent using the same format as ScalarEncoding.writeLong(), but
         * with 2 additional bits in the header to describe the signum of the decimal. We use the following
         * mapping for signum values:
         *
         * null ->  0
         * -1   ->  1
         * 0    ->  2
         * 1    ->  3
         *
         */
        byte extraHeader = (unscaled.signum() < 0 ? HEADER_NEG : HEADER_POS);
        byte[] expBytes = ScalarEncoding.writeLong(exp,extraHeader,HEADER_SIZE_BITS);
        int expLength = expBytes.length;

        //our string encoding only requires 1 byte for 2 digits
        int length = (precision + 1) >>> 1;

        data = new byte[expLength + length + 1];
        System.arraycopy(expBytes, 0, data, 0, expBytes.length);

        String sigString = unscaled.abs().toString(); //strip negatives off if necessary

        char[] sigChars = sigString.toCharArray();
        for (int pos = 0; pos < length; pos++) {
            byte bcd = 0;
            int strPos = 2 * pos;
            if (strPos < precision)
                bcd = (byte) (1 + Character.digit(sigChars[strPos], 10) << 4);
            strPos++;
            if (strPos < precision)
                bcd |= (byte) (1 + Character.digit(sigChars[strPos], 10));
            data[expLength + pos] = bcd;
        }

        data[data.length - 1] = 1;
        if (value.signum() < 0) {
            for (int z = 0; z < data.length; z++) {
                if (z == 0) {
                    data[z] ^= ORDER_FLIP_EXCLUDE_HEADER_MASK;
                } else {
                    data[z] ^= ORDER_FLIP_MASK;
                }
            }
        }

        return data;
    }

    public static BigDecimal toBigDecimal(byte[] data, boolean desc) {
        return toBigDecimal(data, 0, data.length, desc);
    }

    public static BigDecimal toBigDecimal(byte[] data, int dataOffset, int dataLength, boolean desc) {
        int mask = desc ? ORDER_FLIP_MASK : 0;
        int h = ((data[dataOffset] ^ mask) & 0xff) >>> (Byte.SIZE - HEADER_SIZE_BITS);
        if (h == HEADER_NULL) return null;
        if (h == HEADER_ZERO) return BigDecimal.ZERO;

        dataLength -= 1;    // skip trailing 1

        boolean negative = (h == HEADER_NEG);
        mask = desc ^ negative ? ORDER_FLIP_MASK : 0;

        long[] expOffset = ScalarEncoding.readLong(data, dataOffset, desc ^ negative, HEADER_SIZE_BITS);
        long exp = expOffset[0];
        int offset = (int) expOffset[1];

        int length = (dataLength - offset) * 2;
        if (((data[dataOffset + dataLength - 1] ^ mask) & 0xf) == 0) {
            length -= 1;
        }

        int scale = (int) (exp - length + 1);

        // Deserialize the digits into long
        long longValue = 0;
        final long maxVal = (Long.MAX_VALUE - 9) / 10;
        boolean done = true;
        for (int i = 0, shift = 4; i < length; ++i, shift = 4 - shift) {
            if (longValue > maxVal) {     // overflow
                done = false;
                break;
            }
            int digit = ((data[dataOffset + offset + i/2] ^ mask) >>> shift) & 0xf;
            longValue = longValue * 10 + digit - 1;
        }
        if (done) {
            return new BigDecimal(negative ? -longValue : longValue).scaleByPowerOfTen(scale);
        }

        // Deserialize the digits into char[]
        char[] chars = new char[length + 1];
        int pos = 0;
        chars[pos++] = negative ? '-' : '+';
        for (int i = 0, shift = 4; i < length; ++i, shift = 4 - shift) {
            int digit = ((data[dataOffset + offset + i/2] ^ mask) >>> shift) & 0xf;
            chars[pos++] = (char) ('0' + digit - 1);
        }
        return new BigDecimal(chars).scaleByPowerOfTen(scale);
    }
}
