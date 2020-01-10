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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.util.streams;

/**
 * A looping alphabet, returning characters.
 *
 * The alphabet loops over a list of characters. The alphabet-object is used
 * by looping readers, which in turn is used for testing methods requiring
 * streaming inputs.
 *
 * The following alphabets have been defined:
 * <ul><li><em>Modern latin, lowercase</em> ; letters a - z (26)
 *     <li><em>Norwegian/Danish, lowercase</em> ; letters a - z, plus three
 *         additional letters (29)
 *     <li><em>Tamil</em> ; 46 Tamil letters from UNICODE U0B80
 *     <li><em>CJK subset</em> ; 12 letter from UNICODE CJK U4E00 
 * </ul>
 */
public class CharAlphabet {
    
    /** Modern latin, lowercase; a - z, 26 letters */
    public static char[] MODERNLATINLOWER = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
        };

    /** Norwegian/Danish alphabet, lowercase; 29 letters */
    public static char[] NO_DK_LOWER = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '\u00E6', '\u00F8', '\u00E5'
        };

    /** Subset of Tamil alphabet; 46 letters, UNICODE U0B80 */
    public static char[] TAMIL = {
            '\u0B85', '\u0B86', '\u0B87', '\u0B88', '\u0B89', '\u0B8A',
            '\u0B8E', '\u0B8F', '\u0B90', '\u0B92', '\u0B93', '\u0B94',
            '\u0B95', '\u0B99', '\u0B9A', '\u0B9C', '\u0B9E', '\u0B9F',
            '\u0BA3', '\u0BA4', '\u0BA8', '\u0BA9', '\u0BAA', '\u0BAE',
            '\u0BAF', '\u0BB0', '\u0BB1', '\u0BB2', '\u0BB3', '\u0BB4',
            '\u0BB5', '\u0BB6', '\u0BB7', '\u0BB8', '\u0BB9', '\u0BBE',
            '\u0BBF', '\u0BC0', '\u0BC1', '\u0BC2', '\u0BC6', '\u0BC7',
            '\u0BC8', '\u0BCA', '\u0BCB', '\u0BCC'
        };

    /** CJK subset; 12 letters, UNICODE U4E00 */
    public static char[] CJKSUBSET = {
            '\u4E00', '\u4E01', '\u4E02', '\u4E03', '\u4E04', '\u4E05',
            '\u4E06', '\u4E07', '\u4E08', '\u4E09', '\u4E0A', '\u4E0B'
        };

    /**
     * Get a modern latin lowercase alphabet.
     */
    public static CharAlphabet modernLatinLowercase() {
        return new CharAlphabet("Modern latin lowercase",
                                CharAlphabet.MODERNLATINLOWER);
    }

    /**
     * Get a CJK subset alphabet.
     */
    public static CharAlphabet cjkSubset() {
        return new CharAlphabet("CJK subset",
                                CharAlphabet.CJKSUBSET);
    }

    /**
     * Get a Tamil alphabet
     */
    public static CharAlphabet tamil() {
        return new CharAlphabet("Tamil", CharAlphabet.TAMIL);
    }

    /**
     * Get an alphabet consisting of a single character.
     */
    public static CharAlphabet singleChar(char ch) {
        return new CharAlphabet("Single char: " + ch, new char[] { ch });
    }

    /** Name of the alphabet. */
    private final String name;
    /** Characters in the alphabet. */
    private final char[] chars;
    /** Number of characters in the alphabet. */
    private final int charCount;
    /** Current offset into the alphabet/character array. */
    private int off = 0;
    
    /**
     * Create an alphabet with the given name and characters.
     *
     * @param name name of the alphabet
     * @param chars characters in the alphabet.
     */
    private CharAlphabet(String name, char[] chars) {
        this.name = name;
        this.chars = chars;
        this.charCount = chars.length;
    }

    /**
     * Return the name of the alphabet.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return the number of characters in the alphabet.
     */
    public int charCount() {
        return this.charCount;
    }

    /**
     * Return the next char as an <code>integer</code>.
     *
     * @return the next character in the alphabet as an <code>integer</code>
     */
    public int nextCharAsInt() {
        if (off >= charCount) {
            off = 0;
        }
        return (int)chars[off++];
    }

    /**
     * Return the next char.
     *
     * @return the next character in the alphabet
     */
    public char nextChar() {
        if (off >= charCount) {
            off = 0;
        }
        return chars[off++];
    }

    /**
     * Compute the next character to read after reading the specified number
     * of characters. 
     *
     * Besides from returning the index, the internal state of
     * the alphabet is updated.
     *
     * @param charsRead the number of characters read
     * @return the index of the next character
     */
    public int nextCharToRead(int charsRead) {
        off = (off + (charsRead % charCount)) % charCount;
        return off;
    }

    /**
     * Reset the alphabet, the next character returned will be the first
     * character in the alphabet.
     */
    public void reset() {
        off = 0;
    }

    /**
     * Returns a clone of the alphabet.
     *
     * @return A clone.
     */
    public CharAlphabet getClone() {
        return new CharAlphabet(name, chars);
    }

    /**
     * Returns a friendlier textual representation of the alphabet.
     */
    public String toString() {
        return (name + "@" + hashCode() + "(charCount=" + charCount + ")");
    }

} // Enc class CharAlphabet
