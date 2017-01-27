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
package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import com.splicemachine.dbTesting.junit.BaseTestCase;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.db.client.am.ByteArrayCombinerStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Test functionality of <code>ByteArrayCombinerStream</code>.
 */
public class ByteArrayCombinerStreamTest
    extends BaseTestCase {

    private static final byte[] defaultArray = {
            65,66,67,68,69,70,71,72,73,74,75,76,
            77,78,79,80,81,82,83,84,85,86,87,88};

    private ByteArrayCombinerStream combiner;

    public ByteArrayCombinerStreamTest(String name) {
        super(name);
    }

    public void testCombineNullRead()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, 0);
        assertEquals(-1, combiner.read());
    }

    public void testCombineNullReadArray()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, 0);
        assertEquals(-1, combiner.read(new byte[10], 0, 10));
    }

    public void testCombineAvailableNull()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, 0);
        assertEquals(0, combiner.available());
    }

    public void testCombineAvailable4bytes()
            throws IOException {
        byte[] array = {65,66,77,79};
        ArrayList list = new ArrayList(1);
        list.add(array);
        combiner = new ByteArrayCombinerStream(list, 4);
        assertEquals(4, combiner.available());
    }

    /**
     * Make sure an extra "empty" array doesn't cause errors.
     * This test is based on knowledge of the implementation, where an extra
     * byte array was not removed during the reducation process. This can
     * cause <code>nextArray</code> to not return <code>null</code> when it
     * should, causing an <code>ArrayIndexOutOfBoundsException</code>.
     * This bug was corrected by DERBY-1417.
     */
    public void testCombineWithExtraEmptyByteArray()
            throws IOException {
        byte[] array = {65,66,77,79};
        ArrayList list = new ArrayList(2);
        list.add(array);
        list.add(new byte[4]);
        combiner = new ByteArrayCombinerStream(list, array.length);
        byte[] resArray = new byte[array.length];
        assertEquals(array.length,
                     combiner.read(resArray, 0, resArray.length));
        assertTrue(combiner.read() == -1);
    }

    public void testCombineOneArray()
            throws IOException {
        ArrayList list = new ArrayList(1);
        list.add(defaultArray);
        combiner = new ByteArrayCombinerStream(list, defaultArray.length);
        byte[] resArray = new byte[defaultArray.length];
        assertEquals(defaultArray.length,
                     combiner.read(resArray,0, resArray.length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(defaultArray, resArray));
    }

    public void testCominbe100SmallArrays()
            throws IOException {
        int arrays = 100;
        byte[] array = {65,66,77,79};
        ArrayList list = new ArrayList(arrays);
        long length = 0;
        for (int i=0; i < arrays; i++) {
            list.add(array);
            length += array.length;
        }
        byte[] targetArray = new byte[(int)length];
        int offset = 0;
        for (int i=0; i < arrays; i++) {
            System.arraycopy(array, 0, targetArray, offset, array.length);
            offset += array.length;
        }
        combiner = new ByteArrayCombinerStream(list, length);
        byte[] resArray = new byte[(int)length];
        assertEquals(length, combiner.read(resArray, 0, resArray.length));
        assertTrue(combiner.read() == -1);
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }

    public void testTruncateDataFromOneArray()
            throws IOException {
        int length = defaultArray.length -5;
        ArrayList list = new ArrayList(1);
        list.add(defaultArray);
        byte[] targetArray = new byte[length];
        System.arraycopy(defaultArray, 0,
                         targetArray, 0, length);
        byte[] resArray = new byte[length];
        combiner = new ByteArrayCombinerStream(list, length);
        assertEquals(length, combiner.read(resArray, 0, length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }

    public void testTruncateDataFromTwoArrays()
            throws IOException {
        int length = (defaultArray.length *2) -7;
        ArrayList list = new ArrayList(2);
        list.add(defaultArray);
        list.add(defaultArray);
        byte[] targetArray = new byte[length];
        System.arraycopy(defaultArray, 0,
                         targetArray, 0, defaultArray.length);
        System.arraycopy(defaultArray, 0,
                         targetArray, defaultArray.length,
                         length - defaultArray.length);
        byte[] resArray = new byte[length];
        combiner = new ByteArrayCombinerStream(list, length);
        assertEquals(length, combiner.read(resArray, 0, length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }

    /**
     * Make sure an exception is thrown if there is less data available than
     * the specified length.
     */
    public void testTooLittleDataNoCombine() {
        ArrayList list = new ArrayList(1);
        list.add(new byte[5]);
        try {
            combiner = new ByteArrayCombinerStream(list, 10);
            fail("An IllegalArgumentException singalling too little data " +
                    "should have been thrown");
        } catch (IllegalArgumentException iae) {
            // This should happen, continue.
        }
    }

    /**
     * Make sure an exception is thrown if there is less data available than
     * the specified length.
     */
    public void testTooLittleDataWithCombine() {
        ArrayList list = new ArrayList(3);
        byte[] data = {65,66,67,68,69};
        list.add(data);
        list.add(data);
        list.add(data);
        try {
            combiner = new ByteArrayCombinerStream(list, data.length *3 + 1);
            fail("An IllegalArgumentException singalling too little data " +
                    "should have been thrown");
        } catch (IllegalArgumentException iae) {
            // This should happen, continue.
        }
    }

    /**
     * Make sure an exception is thrown if a negative length is specified.
     */
    public void testNegativeLengthArgument() {
        ArrayList list = new ArrayList(1);
        list.add(new byte[1234]);
        try {
            combiner = new ByteArrayCombinerStream(list, -54);
            fail("An IllegalArgumentException singalling negative length " +
                    "should have been thrown");
        } catch (IllegalArgumentException iae) {
            // This should happen, continue.

        }
    }

    /**
     * Demonstrate that the stream does not change negative values in the
     * underlying data.
     * This can cause code to believe the stream is exhausted even though it is
     * not.
     */
    public void testNegativeValueInDataCausesEndOfStream()
            throws IOException {
        byte[] data = {66,67,-123,68,69};
        byte[] targetData = {66,67,0,0,0};
        byte[] resData = new byte[5];
        ArrayList list = new ArrayList(1);
        list.add(data);
        combiner = new ByteArrayCombinerStream(list, data.length);
        byte b;
        int index = 0;
        while ((b = (byte)combiner.read()) > 0) {
            resData[index++] = b;
        }
        assertTrue(Arrays.equals(targetData, resData));
        // Even though we think the stream is exhausted, it is not...
        assertEquals(data[3], (byte)combiner.read());
        assertEquals(data[4], (byte)combiner.read());
        assertEquals(-1, (byte)combiner.read());
    }

    public static Test suite() {
        return new TestSuite(ByteArrayCombinerStreamTest.class,
                             "ByteArrayCombinerStreamTest");
    }
} // End class ByteArrayCombinerStreamTest
