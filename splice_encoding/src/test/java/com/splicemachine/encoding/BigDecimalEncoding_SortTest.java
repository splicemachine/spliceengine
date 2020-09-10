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

import splice.com.google.common.collect.Lists;
import com.splicemachine.primitives.Bytes;
import org.junit.Test;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BigDecimalEncoding_SortTest {

    @Test
    public void sort_positiveValue_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal(".5555555555"),
                new BigDecimal(".55555555555"),
                new BigDecimal(".555555555555"),
                new BigDecimal(".5555555555555"),
                new BigDecimal(".55555555555555"),
                new BigDecimal(".555555555555555"),
                new BigDecimal(".5555555555555555"),
                new BigDecimal(".5555555555555555555555555555555555")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_positiveValue_positiveExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555"),
                new BigDecimal("555555555"),
                new BigDecimal("5555555555"),
                new BigDecimal("55555555555"),
                new BigDecimal("555555555555"),
                new BigDecimal("5555555555555"),
                new BigDecimal("55555555555555"),
                new BigDecimal("555555555555555"),
                new BigDecimal("5555555555555555"),
                new BigDecimal("5555555555555555555555555555555555")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_negativeValue_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-.5555555555555555555555555555555555"),
                new BigDecimal("-.5555555555555555"),
                new BigDecimal("-.555555555555555"),
                new BigDecimal("-.55555555555555"),
                new BigDecimal("-.5555555555555"),
                new BigDecimal("-.555555555555"),
                new BigDecimal("-.55555555555"),
                new BigDecimal("-.5555555555"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_negativeValue_positiveExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-5555555555555555555555555555555555"),
                new BigDecimal("-5555555555555555"),
                new BigDecimal("-555555555555555"),
                new BigDecimal("-55555555555555"),
                new BigDecimal("-5555555555555"),
                new BigDecimal("-555555555555"),
                new BigDecimal("-55555555555"),
                new BigDecimal("-5555555555"),
                new BigDecimal("-555555555"),
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_positiveValues_positiveExponents_AND_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_negativeValues_positiveExponents_AND_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5")
        );
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_negativeAndPositiveValues_negativeAndPositiveExponents_multipleValuesWithSameExponent() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-6"),
                new BigDecimal("-5"),

                new BigDecimal("-.5"),
                new BigDecimal("-.05"),
                new BigDecimal("-.006"),
                new BigDecimal("-.005"),

                new BigDecimal("0"),

                new BigDecimal(".005"),
                new BigDecimal(".006"),
                new BigDecimal(".05"),
                new BigDecimal(".5"),

                new BigDecimal("5"),
                new BigDecimal("6"),
                new BigDecimal("55"),
                new BigDecimal("555"));
        assertSort(bigDecimalList);
    }

    @Test
    public void sort_extraLargeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-1e5000"),
                new BigDecimal("-1e1000"),
                new BigDecimal("-1e100"),
                new BigDecimal("-1e10"),
                new BigDecimal("-1e1"),

                new BigDecimal("-1e-1"),
                new BigDecimal("-1e-10"),
                new BigDecimal("-1e-100"),
                new BigDecimal("-1e-1000"),
                new BigDecimal("-1e-5000"),

                new BigDecimal("1e-5000"),
                new BigDecimal("1e-1000"),
                new BigDecimal("1e-100"),
                new BigDecimal("1e-10"),
                new BigDecimal("1e-1"),

                new BigDecimal("1e1"),
                new BigDecimal("1e10"),
                new BigDecimal("1e100"),
                new BigDecimal("1e1000"),
                new BigDecimal("1e5000")
        );
        assertSort(bigDecimalList);
    }

    public void assertSort(List<BigDecimal> originalList) {
        List<BigDecimal> ascendingList = Lists.newArrayList(originalList);
        List<BigDecimal> descendingList = Lists.newArrayList(originalList);

        Collections.sort(ascendingList);
        Collections.reverse(descendingList);

        // assert original list (test data) was in ascending order
        assertEquals(originalList, ascendingList);

        assertStuff(ascendingList, false);
        assertStuff(descendingList, true);
    }

    public void assertStuff(List<BigDecimal> bigDecimalList, boolean descending) {
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList, descending);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BASE_COMPARATOR);
        List<BigDecimal> decodedList = EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes, descending);
        assertEquals("descending=" + descending, bigDecimalList, decodedList);
    }


}
