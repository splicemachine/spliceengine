/*  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.gotometrics.orderly;

import java.io.IOException;

import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;

/**
 * Serializes and deserializes BytesWritable into a sortable variable length representation with an optional fixed
 * length prefix (on which no encoding will be applied).
 *
 * <h1>Serialization Format</h1>
 * <p/>
 * Variable length byte arrays can not be written simply as the original byte array, because this would leave us
 * without any usable byte (or byte sequence) to mark the end of the byte array. We can also not simply write
 * the length of the array in a fixed number of bytes in the beginning of the output, because this would mean
 * the rowkeys can no longer be sorted and "prefix searched". This impacts things like splitting tables over
 * regions etc.
 * <p/>
 * Therefor we use a variant of packed binary coded decimal (BCD). We start by interpreting the byte array
 * as an unsigned variable length integer number represented in decimal format (using only digits 0 - 9).
 * Packed BCD encodes each digit (0 - 9) into a 4 bit nibble (binary 0000 - 1001) and "packs" two of these nibbles
 * into one byte.
 * <p/>
 * BCD only uses the byte range (binary) 00000000 - 10011001. This results in an uneven distribution of values
 * over the whole possible range, resulting in problems with splitting tables over regions based on predefined
 * byte ranges for each region. To solve this, we modified BCD with a mapping from digits to 4 bit values
 * which uses the range more evenly. Furthermore, we reserve a number of small nibbles for special purposes.
 * <p/>
 * The modified mapping is:
 * <pre>
 * digit 0 = binary 0011 (0x03)
 * digit 1 = binary 0100 (0x04)
 * digit 2 = binary 0101 (0x05)
 * digit 3 = binary 0110 (0x06)
 * digit 4 = binary 0111 (0x07)
 * digit 5 = binary 1001 (0x09)
 * digit 6 = binary 1010 (0x0A)
 * digit 7 = binary 1100 (0X0C)
 * digit 8 = binary 1110 (0x0E)
 * digit 9 = binary 1111 (0x0F)
 * </pre>
 *
 * This also means that a number of values are "available" as terminator, filler and to encode NULL values , we've
 * chosen the nibble 0000 for NULL values and the nibble 0001 as terminator. This ensures that shorter byte arrays that
 * are the prefix of a longer byte array will always compare less than the longer string, as the terminator is a
 * smaller value that any decimal digit.
 * <p/>
 * Furthermore, we also want that if we encode two byte arrays of the same length, that the resulting encoded byte
 * arrays are also of the same length and that their byte order is the same as the byte order of the original byte
 * arrays. Also, when encoding two byte arrays of different lengths, the byte order of the encoded byte arrays should
 * be the same as of the original byte arrays. To accomplish this, we use a filler character such that all encoding is
 * always using the same number of encoded bytes per original byte (i.e. 3 nibbles per original byte, so a loss of
 * 50%).
 * The filler nibble should be smaller than any real encoded value, but bigger than the nibbles used for zero and NULL.
 * <p/>
 * To encode a NULL, we output 0x00 and return. Otherwise, to encode a non-NULL
 * byte array we BCD encode the byte array and then append the
 * terminator nibble at the end. Decoding is simply the reverse of the above
 * operations.
 * <p/>
 * The fixed length prefix will be written as is before the variable length part. This can be useful to prevent BCD
 * encoding on a fixed parth (which you know will always be there anyways).
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending
 * sort, except we logically invert (take the 1's complement of) each byte,
 * including the null and termination bytes.
 *
 * <h1> Implicit Termination </h1>
 * If {@link #termination} is false and the sort order is ascending, we
 * encode NULL values as a zero-length byte array, and omit the terminator byte
 * for every byte array except the empty byte array. Implicit termination is
 * discussed further in {@link RowKey}.
 */
public class VariableLengthBytesWritableRowKey extends RowKey {

    private static final byte NULL = 0x00;

    private static final byte TERMINATOR_NIBBLE = 0x01;

    private static final byte TWO_TERMINATOR_NIBBLES = 0x11;

    private static final char FILLER = 'f'; // whatever, just not a 0-9 digit

    private static final byte FILLER_NIBBLE = 0x02;

    private final int fixedPrefixLength;

    public VariableLengthBytesWritableRowKey() {
        // no fixed part by default
        this(0);
    }

    public VariableLengthBytesWritableRowKey(int fixedPrefixLength) {
        if (fixedPrefixLength < 0)
            throw new IllegalArgumentException("fixed prefix length can not be < 0");

        this.fixedPrefixLength = fixedPrefixLength;
    }

    @Override
    public Class<?> getSerializedClass() {
        return BytesWritable.class;
    }

    @Override
    public int getSerializedLength(Object o) throws IOException {
        if (o == null)
            return terminate() ? fixedPrefixLength + 1 : fixedPrefixLength;

        final BytesWritable input = (BytesWritable) o;
        return fixedPrefixLength + getSerializedLength(
                toStringRepresentation(input.getBytes(), fixedPrefixLength, input.getLength() - fixedPrefixLength));
    }

    /**
     * @return the length of a String with digits if serialized in our customized BCD format. We require
     *         1 byte for every 2 characters, rounding up. Furthermore, if the number
     *         of characters is even, we require an additional byte for the
     *         terminator nibble if terminate() is true.
     */
    private int getSerializedLength(String s) {
        if (terminate())
            return (s.length() + 2) / 2;
        else
            return s.length() == 0 ? 1 : (s.length() + 1) / 2;
    }

    @Override
    public void serialize(Object o, ImmutableBytesWritable bytesWritable) throws IOException {
        byte[] bytesToWriteIn = bytesWritable.get();
        int offset = bytesWritable.getOffset();

        if (o == null) {
            if (fixedPrefixLength > 0)
                throw new IllegalStateException("excepted at least " + fixedPrefixLength + " bytes to write");
            else if (terminate()) {
                // write one (masked) null byte
                bytesToWriteIn[offset] = mask(NULL);
                RowKeyUtils.seek(bytesWritable, 1);
            }
        } else {
            final BytesWritable input = (BytesWritable) o;
            if (fixedPrefixLength > input.getLength())
                throw new IllegalStateException("excepted at least " + fixedPrefixLength + " bytes to write");
            else {
                encodeFixedPrefix(input.getBytes(), bytesWritable);
                encodedCustomizedReversedPackedBcd(toStringRepresentation(input.getBytes(), fixedPrefixLength,
                        input.getLength() - fixedPrefixLength),
                        bytesWritable);
            }
        }
    }

    private void encodeFixedPrefix(byte[] input, ImmutableBytesWritable bytesWritable) {
        final byte[] output = bytesWritable.get();
        final int offset = bytesWritable.getOffset();
        for (int i = 0; i < fixedPrefixLength; i++) {
            output[offset + i] = mask(input[i]);
        }

        RowKeyUtils.seek(bytesWritable, fixedPrefixLength);
    }

    private String toStringRepresentation(byte[] bytes, int offset, int length) {
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            byte aByte = bytes[offset + i];
            // An unsigned byte results in max 3 decimal digits (because max value is "255") and we want to use
            // always the max size such that if two byte arrays have the same length,
            // the two encoded byte arrays also have the same length (such that byte sort order is remained).
            // Therefore, we fill the gaps with a filler character.
            result.append(prependZeroes(3, "" + UnsignedBytes.toInt(aByte)));
        }

        return result.toString();
    }

    /**
     * Prepend a string with zeroes up to the given total length. Note that we could have used String.format("%03d",
     * ...) here but this turned out to be a performance killer!
     *
     * @param totalLength length of the returned string
     * @param string      string to prepend with zeroes
     * @return zero prepended string
     */
    private String prependZeroes(int totalLength, String string) {
        if (string.length() >= totalLength) {
            // no need to prepend anything
            return string;
        } else {
            // prepend with zeroes up to requested total length
            final StringBuilder zeroes = new StringBuilder(totalLength - string.length());
            for (int i = 0; i < totalLength - string.length(); i++) {
                zeroes.append("0");
            }

            return zeroes.toString() + string;
        }
    }

    private byte[] fromStringRepresentation(String string) {
        // each 3 digits correspond to an encoded byte
        final byte[] result = new byte[string.length() / 3];

        // process the string per 3 digits
        final char[] digits = string.toCharArray();
        for (int i = 0; i < result.length; i++) {
            int digitIdx = i * 3;
            StringBuilder singleByteBcdString = new StringBuilder();
            for (int j = 0; j < 3; j++) {
                singleByteBcdString.append(digits[digitIdx + j] == FILLER ? "" : digits[digitIdx + j]);
            }

            result[i] = (byte) Integer.parseInt(singleByteBcdString.toString());
        }

        return result;
    }

    @Override
    public void skip(ImmutableBytesWritable bytesWritable) throws IOException {
        if (bytesWritable.getLength() <= 0)
            return;

        byte[] bytes = bytesWritable.get();
        int offset = bytesWritable.getOffset();
        int len = bytesWritable.getLength();

        RowKeyUtils.seek(bytesWritable,
                fixedPrefixLength + getBcdEncodedLength(bytes, offset + fixedPrefixLength, len - fixedPrefixLength));
    }

    protected int getBcdEncodedLength(byte[] bytes, int offset, int len) {

        int i = 0;
        while (i < len) {
            byte c = mask(bytes[offset + i++]);
            if ((c & 0x0f) == TERMINATOR_NIBBLE)
                break;
        }

        return i;
    }

    @Override
    public Object deserialize(ImmutableBytesWritable bytesWritable) throws IOException {
        final int length = bytesWritable.getLength();

        if (length <= 0 && fixedPrefixLength == 0)
            return null;

        final int offset = bytesWritable.getOffset();
        final int variableLengthSuffixOffset = offset + fixedPrefixLength;
        final int variableLengthSuffixLength = length - fixedPrefixLength;

        final byte[] fixedLengthPrefix = decodeFixedPrefix(bytesWritable);

        final byte[] variableLengthSuffix = fromStringRepresentation(
                decodeCustomizedReversedPackedBcd(bytesWritable, variableLengthSuffixOffset,
                        variableLengthSuffixLength));

        return new BytesWritable(merge(fixedLengthPrefix, variableLengthSuffix));
    }

    private static byte[] merge(byte[] array1, byte[] array2) {
        byte[] merged = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, merged, 0, array1.length);
        System.arraycopy(array2, 0, merged, array1.length, array2.length);
        return merged;
    }

    private byte[] decodeFixedPrefix(ImmutableBytesWritable input) {
        final byte[] output = new byte[fixedPrefixLength];

        final byte[] inputBytes = input.get();
        final int offset = input.getOffset();
        for (int i = 0; i < fixedPrefixLength; i++) {
            output[i] = mask(inputBytes[offset + i]);
        }

        RowKeyUtils.seek(input, fixedPrefixLength);
        return output;
    }


    private static byte[] CUSTOMIZED_BCD_ENC_LOOKUP = new byte[]{3, 4, 5, 6, 7, 9, 10, 12, 14, 15};

    // note that the value -1 means invalid
    private static byte[] CUSTOMIZED_BCD_DEC_LOOKUP = new byte[]{-1, -1, -1, 0, 1, 2, 3, 4, -1, 5, 6, -1, 7, -1, 8, 9};

    /**
     * Encodes a String with digits into a "customized reversed packed binary coded decimal" byte array.
     */
    void encodedCustomizedReversedPackedBcd(String decimalDigits, ImmutableBytesWritable bytesWritable) {
        byte[] bytes = bytesWritable.get();
        int offset = bytesWritable.getOffset();
        final int encodedLength = getSerializedLength(decimalDigits);
        final char[] digits = decimalDigits.toCharArray();

        for (int i = 0; i < encodedLength; i++) {
            byte bcd = TWO_TERMINATOR_NIBBLES; // initialize with termination nibbles
            int digitsIdx = 2 * i;
            boolean firstNibbleWritten = false;
            if (digitsIdx < digits.length) {
                bcd = (byte) (lookupDigit(digits[digitsIdx]) << 4);
                firstNibbleWritten = true;
            }
            if (++digitsIdx < digits.length) {
                bcd |= lookupDigit(digits[digitsIdx]);
            } else if (firstNibbleWritten) {
                bcd |= TERMINATOR_NIBBLE; // uneven number of digits -> write terminator nibble
            }
            bytes[offset + i] = mask(bcd); // this could be two bcd nibbles or two terminator nibbles
        }

        RowKeyUtils.seek(bytesWritable, encodedLength);
    }

    private byte lookupDigit(char digit) {
        if (digit != FILLER)
            return CUSTOMIZED_BCD_ENC_LOOKUP[Character.digit(digit, 10)];
        else
            return FILLER_NIBBLE;
    }

    /**
     * Decodes a "customized reversed packed binary coded decimal" byte array into a String with digits.
     *
     * @param bytesWritable the customized packed BCD encoded byte array
     * @return The decoded value
     */
    String decodeCustomizedReversedPackedBcd(ImmutableBytesWritable bytesWritable, int offset, int length) {
        int i = 0;
        final byte[] bytes = bytesWritable.get();

        StringBuilder sb = new StringBuilder();
        while (i < length) {
            byte c = mask(bytes[offset + i++]);
            if (addDigit((byte) ((c >>> 4) & 0x0f), sb) || addDigit((byte) (c & 0x0f), sb))
                break;
        }

        RowKeyUtils.seek(bytesWritable, i);
        return sb.toString();
    }

    /**
     * Decodes a Binary Coded Decimal digit and adds it to a string. Returns
     * true (and leaves string unmodified) if digit is the terminator nibble.
     * Returns false otherwise.
     */
    protected boolean addDigit(byte bcd, StringBuilder sb) {
        if (bcd == TERMINATOR_NIBBLE) {
            return true;
        } else {
            if (bcd != FILLER_NIBBLE)
                sb.append(CUSTOMIZED_BCD_DEC_LOOKUP[bcd]);
            else
                sb.append(FILLER);
            return false;
        }
    }

}
