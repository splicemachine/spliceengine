package com.splicemachine.derby.hbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpliceWriteControlTest {

    @Test
    public void performIndependentWrite() {
        SpliceWriteControl writeControl = new SpliceWriteControl(3, 3, 200, 200);

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=2, dependentWriteCount=0, independentWriteCount=50 }", writeControl.getWriteStatus().toString());

        writeControl.finishIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());
    }

}