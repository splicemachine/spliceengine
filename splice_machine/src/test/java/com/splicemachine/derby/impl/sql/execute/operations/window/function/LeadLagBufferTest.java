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

package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link com.splicemachine.derby.impl.sql.execute.operations.window.function.LeadLagFunction.LeadLagBuffer}
 */
@Category(ArchitectureIndependent.class)
public class LeadLagBufferTest {

    @SuppressWarnings("FieldCanBeLocal")
    private boolean debug = false;

    //================================================================================================
    // Lead Buffer
    //================================================================================================

    @Test
    public void testLeadBufferLifecycle() throws Exception {
        helpTestBufferLifecyle(new LeadLagFunction.LeadBuffer());
    }

    @Test
    public void testLeadMinus1() throws Exception {
        int offset = -1;
        int frameSize = 20;


        try {
            helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
            fail("Expected exception - offset can't be negative.");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testLead0() throws Exception {
        int offset = 0;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLead1() throws Exception {
        int offset = 1;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLead3() throws Exception {
        int offset = 3;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadHalfMinus1() throws Exception {
        int offset = 9;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadHalf() throws Exception {
        int offset = 10;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadHalfPlus1() throws Exception {
        int offset = 11;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadFrameSizeMinus1() throws Exception {
        int offset = 19;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadFrameSize() throws Exception {
        int offset = 20;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    @Test
    public void testLeadFrameSizePlus1() throws Exception {
        int offset = 21;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LeadBuffer(), frameSize, offset);
    }

    //================================================================================================
    // LagBuffer
    //================================================================================================

    @Test
    public void testLagBufferLifecycle() throws Exception {
        helpTestBufferLifecyle(new LeadLagFunction.LagBuffer());
    }

    @Test
    public void testLagMinus1() throws Exception {
        int offset = -1;
        int frameSize = 20;

        try {
            helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
            fail("Expected exception - offset can't be negative.");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testLag0() throws Exception {
        int offset = 0;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    @Test
    public void testLag1() throws Exception {
        int offset = 1;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    @Test
    public void testLag3() throws Exception {
        int offset = 3;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    @Test
    public void testLagFrameSizeMinus1() throws Exception {
        int offset = 19;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    @Test
    public void testLagFrameSize() throws Exception {
        int offset = 20;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    @Test
    public void testLagFrameSizePlus1() throws Exception {
        int offset = 21;
        int frameSize = 20;

        helpTestLeadLagBuffer(new LeadLagFunction.LagBuffer(), frameSize, offset);
    }

    //================================================================================================
    // Test help
    //================================================================================================

    private void helpTestBufferLifecyle(LeadLagFunction.LeadLagBuffer buff) {
        List<DataValueDescriptor> result = buff.terminate();
        assertNotNull(result);
        assertEquals(0, result.size());

        buff.terminate();
        buff.initialize(5);
        result = buff.terminate();
        assertNotNull(result);
        assertEquals("haven't added anything yet", 0, result.size());

        DataValueDescriptor dvd = new SQLInteger(13);
        buff.addRow(dvd, dvd.getNewNull());
        result = buff.terminate();
        assertNotNull(result);
        assertTrue("we don't remove things in terminate(). have to call initialize()", result.size() > 0);

        buff.initialize(5);
        result = buff.terminate();
        assertNotNull(result);
        assertEquals("we've initialized() but haven't added anything yet", 0, result.size());
    }

    private void helpTestLeadLagBuffer(LeadLagFunction.LeadLagBuffer buff, int frameSize, int offset) {
        List<Tuple> expectedResultTuples =  // to gen expected results for lag fn, convert offset to its negative
            genExpectedResults(frameSize, (buff instanceof LeadLagFunction.LagBuffer ? offset*-1 : offset));
        buff.initialize(offset);

        for (int i=1; i<=frameSize; i++) {
            DataValueDescriptor dvd = new SQLInteger(i);
            buff.addRow(dvd, dvd.getNewNull());
        }

        List<DataValueDescriptor> actualResults = buff.terminate();
        // set debug = true to print values
        if (this.debug) {
            printDebug(expectedResultTuples, actualResults);
        }
        assertEquals(expectedResultTuples.size(), actualResults.size());
        for (int i=0; i<frameSize; i++) {
            Tuple current = expectedResultTuples.get(i);
            current.actualResult = actualResults.get(i);
            assertEquals(Tuple.print(expectedResultTuples, i), current.expectedResult, current.actualResult);
        }
    }

    private void printDebug(List<Tuple> expectedResultTuples, List<DataValueDescriptor> actualResults) {
        System.out.println("--------------------------------------------------------------------------");
        System.out.print("Row:      ");
        for (Tuple expectedResultTuple : expectedResultTuples) {
            System.out.print(expectedResultTuple.rowValue);
            System.out.print(", ");
        }
        System.out.println();
        System.out.print("Expected: ");
        for (Tuple expectedResultTuple : expectedResultTuples) {
            System.out.print(expectedResultTuple.expectedResult);
            System.out.print(", ");
        }
        System.out.println();
        System.out.print("Actual:   ");
        for (DataValueDescriptor actualResult : actualResults) {
            System.out.print(actualResult);
            System.out.print(", ");
        }
        System.out.println();
    }

    /**
     * @param frameSize size of the frame
     * @param offset lead or lag offset. Should be negative for lag.
     * @return List of expected result tuples [rowValue, expectedValue, actualValue (will come from buffer)]
     */
    private List<Tuple> genExpectedResults(int frameSize, int offset) {
        List<Tuple> results = new ArrayList<>(frameSize);
        for (int i=1; i<=frameSize; i++) {
            Tuple t = new Tuple(new SQLInteger(i));
            results.add(t);
            int leadLag = i + offset;
            if (leadLag > frameSize || leadLag <= 0) {
                t.expectedResult = t.rowValue.getNewNull();
            } else {
                t.expectedResult = new SQLInteger(leadLag);
            }
        }
        return results;
    }

    private static class Tuple {
        final DataValueDescriptor rowValue;
        DataValueDescriptor expectedResult;
        DataValueDescriptor actualResult;
        Tuple(DataValueDescriptor rowValue) {
            this.rowValue = rowValue;
        }
        @Override
        public String toString() {
            return "Row: "+rowValue+" Expected: "+expectedResult+" Actual: "+actualResult;
        }
        public static String print(List<Tuple> tuples, int badIndex) {
            StringBuilder buf = new StringBuilder();
            for (int i=0; i<tuples.size(); i++) {
                if (i == badIndex) {
                    buf.append(">*>[");
                }
                buf.append(tuples.get(i));
                if (i == badIndex) {
                    buf.append("]<*<");
                }
                buf.append(',');
            }
            if (buf.length() > 0) {
                buf.setLength(buf.length()-1);
            }
            return buf.toString();
        }
    }
}
