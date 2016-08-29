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

import org.spark_project.guava.collect.Lists;
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
