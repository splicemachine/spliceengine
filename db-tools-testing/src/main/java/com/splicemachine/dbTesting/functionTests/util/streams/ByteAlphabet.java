/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.util.streams;

import java.io.UnsupportedEncodingException;

/**
 * A looping alphabet, returning bytes in a specified encoding.
 *
 * The alphabet loops over a list of bytes representing characters. The
 * alphabet-object is used by looping stream, which in turn is used for testing
 * methods requiring streaming inputs.
 *
 * The following alphabets have been defined:
 * <ul><li><em>Modern latin, lowercase</em> ; letters a - z (26)
 *     <li><em>Norwegian/Danish, lowercase</em> ; letters a - z, plus three
 *         additional letters (29)
 *     <li><em>Tamil</em> ; 46 Tamil letters from UNICODE U0B80
 *     <li><em>CJK subset</em> ; 12 letter from UNICODE CJK U4E00
 * </ul>
 */
public class ByteAlphabet {

    /** The name of the alphabet. */
    private final String name;
    /** The encoding used to represent characters as bytes. */
    private final String encoding;
    /** The bytes representing the characters in the alphabet. */
    private final byte[] bytes;
    /** The number of characters in the alphabet. */
    private final int charCount;
    /** The number of byes in the alphabet. */
    private final int byteCount;
    /** Offset into the byte array. */
    private int boff = 0;

    /**
     * Create an alphabet returning bytes representing the lowercase letters
     * a-z in the "US-ASCII" encoding.
     */
    public static ByteAlphabet modernLatinLowercase() {
        return new ByteAlphabet("Modern latin lowercase, US-ASCII",
                            CharAlphabet.MODERNLATINLOWER,
                            "US-ASCII");
    }

    /**
     * Create an alphabet returning bytes representing the 29 lowercase
     * letters in the Norwegian/Danish alphabet in the "ISO-8859-1" encoding.
     */
    public static ByteAlphabet norwegianLowercase() {
        return new ByteAlphabet("Norwegian/Danish lowercase, ISO-8859-1",
                        CharAlphabet.NO_DK_LOWER,
                        "ISO-8859-1");
    }

    /**
     * Create an alphabet returning bytes representing a subset of the Tamil
     * alphabet in the UTF-8 encoding.
     */
    public static ByteAlphabet tamilUTF8() {
        return new ByteAlphabet("Tamil, UTF-8",
                        CharAlphabet.TAMIL,
                        "UTF8");
    }

    /**
     * Create an alphabet returning bytes representing a subset of the Tamil
     * alphabet in the UTF-16BE encoding.
     */
    public static ByteAlphabet tamilUTF16BE() {
        return new ByteAlphabet("Tamil, UTF-16BE",
                        CharAlphabet.TAMIL,
                        "UTF-16BE");
    }

    /**
     * Create an alphabet returning bytes representing a subset of the CJK
     * alphabet in the UTF-8 encoding.
     */
    public static ByteAlphabet cjkSubsetUTF8() {
        return new ByteAlphabet("CJK subset, UTF-8",
                        CharAlphabet.CJKSUBSET,
                        "UTF8");
    }

    /**
     * Create an alphabet returning bytes representing a subset of the CJK
     * alphabet in the UTF-16BE encoding.
     */
    public static ByteAlphabet cjkSubsetUTF16BE() {
        return new ByteAlphabet("CJK subset, UTF-16BE",
                        CharAlphabet.CJKSUBSET,
                        "UTF-16BE");
    }

    /**
     * Create an alphabet that consists of a single byte.
     */
    public static ByteAlphabet singleByte(byte b) {
        return new ByteAlphabet(
                "Single byte: " + b,
                new char[] { (char) (b & 0xff) },
                "US-ASCII");
    }

    /**
     * Create an alphabet with the given name, the given characters and using
     * the specified encoding to represent the characters as bytes.
     *
     * @param name the name of the alphabet
     * @param chars the characters in the alphabet
     * @param encoding the encoding to use to represent characters as bytes
     */
    private ByteAlphabet(String name, char[] chars, String encoding) {
        this.name = name;
        this.encoding = encoding;
        this.charCount = chars.length;
        String tmpStr = new String(chars);
        byte[] tmpBytes;
        try {
            tmpBytes = tmpStr.getBytes(encoding);
        } catch (UnsupportedEncodingException uee) {
            // We are nasty and ignore this...
            tmpBytes = new byte[] {0};
        }
        this.bytes = tmpBytes;
        this.byteCount = tmpBytes.length;
    }

    /**
     * Return the name of the alphabet.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return the encoding used to represent characters as bytes.
     */
    public String getEncoding() {
        return this.encoding;
    }

    /**
     * Return the number of characters in the alphabet.
     */
    public int charCount() {
        return charCount;
    }

    /**
     * Return the number of bytes in the alphabet.
     *
     * The number of bytes in the alphabet is noramlly different from the
     * number of characters in the alphabet, but it depends on the
     * characters in the alphabet and encoding used to represent them as
     * bytes.
     */
    public int byteCount() {
        return byteCount;
    }

    /**
     * Return the next byte in the alphabet.
     */
    public byte nextByte() {
        if (boff >= byteCount) {
            boff = 0;
        }
        return bytes[boff++];
    }

    /**
     * Reset the alphabet, the next byte returned is the first byte in the
     * alphabet, which might not be a complete character.
     */
    public void reset() {
        boff = 0;
    }

    /**
     * Compute the next byte to read after reading the specified number
     * of bytes.
     *
     * Besides from returning the index, the internal state of
     * the alphabet is updated.
     *
     * @param bytesRead the number of bytes read
     * @return the index of the next byte
     */
    public int nextByteToRead(int bytesRead) {
        boff = (boff + (bytesRead % byteCount)) % byteCount;
        return boff;
    }
} // End class ByteAlphabet
