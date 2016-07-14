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

package com.splicemachine.stats.util;

import java.math.BigInteger;

/**
 * Utilities around UTF charsets, and statistics.
 *
 * @author Scott Fines
 *         Date: 8/27/15
 */
public class UTFUtils{

    private UTFUtils(){}

    /*
     * The maximum possible UTF-8 character is 0x10FFFF. Thus, the total number of possible UTF-8 characters
     * in a given location is given by 0x10FFFF, so we use that for our power.
     */
    private static final BigInteger UTF_8_MAX= BigInteger.valueOf(0x10FFFF);

    /**
     * Compute the "UTF-8 position" of the specified string, based on strings with a maximum length of
     * {@code maxStringLength}.
     *
     * The set of all possible strings of bounded length encoded in valid UTF-8 is a finite set (as indeed, any encoded string
     * of bounded length is). Therefore, mathematically, we can construct a map from the String to a unique identifier
     * (which we call the <em>position</em>).
     * Because Strings have a total order, that mapping can indeed preserve the total order of the Strings (i.e. if
     * strA < strB, then utf8Position(strA) < utf8Position(strB)). In fact, there are infinitely many such maps.
     *
     * We choose the "permutative map" here. The "permutative map" encodes the position of the string as the
     * number of possible permutations of strings which compare less than itself. For example, if we have strings
     * of length 2 and an alphabet with the characters "A", "B", and "", then all the strings less than "B" would be
     * """"
     * ""A
     * A""
     * AA
     * which would mean that the "permutative position" of the string "B" is 4 (we count from 0).
     *
     * In general, to compute the permutative position of a lexicographic sequence, use the following algorithm. First,
     * let {@code N} be the total number of characters in the alphabet of interest. Then, let {@code L} be the maximum
     * length of the sequence. Start at the right-most character (assume all strings are of length {@code L}. We will
     * expand this to remove this assumption in a moment). The position of that character is its code point. Now, consider
     * the second-rightmost character. For each possible character in this position, there are {@code N} possible values
     * for the rightmost character. Thus, there are N^2 possible strings of length 2, and the permutative position is
     * written as {@code N*str[0]+str[1]}. Extending this process out recursively leads to the recurrence relation
     *
     * p(s) = permutative position of string s
     * l = min(length(s),L)
     * p(s[0]) = s[0]
     * p(s[0:n]) = p(s[n]) + N*p(s[0:n-1])
     *
     * then to compute {@code p(s)}, we simply compute {@code p(s[l])}.
     *
     * If the length of the string is less than the maximum, then we need to shift over the position by the number
     * of missing characters. This is effectively "padding" the string with the character 0x00 to the width of the string.
     *
     * Note that it is usually more efficient to unroll the recurrence relation into a single for loop, rather than
     *  a recursive invocation like we represent here (the below implementation does such an unrolling).
     *
     * Extending this concept to the UTF-8 charset, we know that there are exactly {@code N=131071} different characters available
     * in the UTF-8 charset. Thus, each character in a string takes on an integer value between 0 and 131071. The
     * maximum such string is the string consisting entirely of the character {@code 0x01FFFF}, while the minimum
     * string consists entirely of the character {@code 0x00}.
     *
     * @param str the string to compute the position of
     * @param maxStringLength the maximum length that strings are allowed to take. If the passed in string has a length
     *                        less than that of {@code maxStringLength}, it will be "padded" mathematically to place
     *                        it within the lexicographic sort order. If the passed in string has a length greater
     *                        than that of {@code maxStringLength}, then any characters after {@code maxStringLength}
     *                        are ignored (they do not contribute to the position.
     * @return the permutative position of {@code str} in the set of all possible UTF-8 strings with
     * length < {@code maxStringLength}
     */
    public static BigInteger utf8Position(String str,int maxStringLength) {
        assert str!=null: "Cannot compute the position of a null element!";
        /*
         * There are N possible UTF-8 characters in each position, so if we represent each character with
         * a single big-endian integer, we can compute the absolute position in the ordering according to the
         * rule
         *
         * N^strLen*str[0] + N^(strLen-1)*str[1]+...+str[strLen-1]
         *
         * Or, written as a recurrence relation, as
         *
         * p[0] = str[0]
         * p[n] = N*p[n-1]+p[n]
         *
         * then we just compute
         * p = p[strLen]
         *
         * note that this isn't really the most efficient way of executing it (the first way is better), but it's
         * easier for us to understand what's going on.
         */
        int len = Math.min(str.length(),maxStringLength);

        BigInteger pos = BigInteger.ZERO;
        for(int codePoint = 0;codePoint<len;){
            int v = str.codePointAt(codePoint);
            pos = BigInteger.valueOf(v).add(UTF_8_MAX.multiply(pos));
            codePoint+=Character.charCount(v);
        }

        /*
         * Strings may be shorter than the maximum length. In that case, we "pad" the string with the 0x00 byte. In
         * effect, this is multiplying the previous computation over by N^(strLen-len).
         */
        if(maxStringLength>len){
            BigInteger scale = UTF_8_MAX.pow(maxStringLength-len);
            pos = pos.multiply(scale);
        }
        return pos;
    }
}
