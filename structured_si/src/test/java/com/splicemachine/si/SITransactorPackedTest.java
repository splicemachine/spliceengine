package com.splicemachine.si;

public class SITransactorPackedTest extends SITransactorTest {
    @Override
    public void setUp() {
        usePacked = true;
        super.setUp();
    }
}
