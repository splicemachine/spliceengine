package com.splicemachine.si;

import org.junit.Test;

import java.io.IOException;

public class SITransactorPackedTest extends SITransactorTest {
    @Override
    public void setUp() {
        usePacked = true;
        super.setUp();
    }

    @Test
    public void writeScanWithDeleteActiveUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanTwoVersionsUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeReadViaFilterResult() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanWithManyDeletesUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }

    @Test
    public void writeScanMixSameRowUncommittedAsOfStart() throws IOException {
        // temporarily mask test in parent class
    }
}
