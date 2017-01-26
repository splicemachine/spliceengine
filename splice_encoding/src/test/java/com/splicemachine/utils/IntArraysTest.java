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

package com.splicemachine.utils;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the IntArrays utility
 *
 * @author Jeff Cunningham
 *         Date: 8/8/14
 */
public class IntArraysTest {

    @Test
    public void testComplementMap() throws Exception {
        int[] expected = new int[] {0,3,4,5};
        int[] input = new int[] {1,2};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap1() throws Exception {
        int[] expected = new int[] {3,4,5};
        int[] input = new int[] {0,1,2};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap2() throws Exception {
        int[] expected = new int[] {0,1,2};
        int[] input = new int[] {3,4,5};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap3() throws Exception {
        int[] expected = new int[] {};
        int[] input = new int[] {0,1,2,3,4,5};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap4() throws Exception {
        int[] expected = new int[] {0,1,2,3,4,5};
        int[] input = new int[] {};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap5() throws Exception {
        int[] expected = new int[] {1,2,3,4};
        int[] input = new int[] {0};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap6() throws Exception {
        int[] expected = new int[] {0,1,2,3};
        int[] input = new int[] {4};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap6a() throws Exception {
        int[] expected = new int[] {0,1,2,3,5};
        int[] input = new int[] {4};
        int size = 6;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap7() throws Exception {
        int[] expected = new int[] {1,2,3,4};
        int[] input = new int[] {0};
        int size = expected.length + input.length-1;

        try {
            IntArrays.complementMap(input,size);
            Assert.fail("Should have been an Assertion error - more missing fields than present.");
        } catch (AssertionError e) {
            // expected
        }
    }

    @Test
    public void testComplementMap8() throws Exception {
        int[] expected = new int[] {0,1,4,5};
        int[] input = new int[] {2,3};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    @Test
    public void testComplementMap9() throws Exception {
        int[] expected = new int[] {0,1,2,5};
        int[] input = new int[] {3,4};
        int size = expected.length + input.length;

        int[] complement = IntArrays.complementMap(input,size);
//        System.out.println(printArrays(input,size,expected,complement));
        Assert.assertArrayEquals(printArrays(input,size,expected,complement),expected,complement);
    }

    // =============================================================
    private String printArrays(int[] input, int size, int[] expected, int[] actual) {
        return "FilterMap: "+Arrays.toString(input) +" Size: "+size+ "\nResult: "+ Arrays.toString(expected) +
            "\nActual:   "+ Arrays.toString(actual)+"\n";
    }
}
