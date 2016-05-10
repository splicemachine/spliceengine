package com.splicemachine.si.impl.hlc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jleach on 4/21/16.
 */
public class HLCTest {

    @Test
    public void alwaysIncreasingLocalEvent() {
        HLC hlc = new HLC();
        long value = 0;
        for (int i = 0; i< 1000000; i++) {
            long comparison = hlc.sendOrLocalEvent();
            Assert.assertTrue("went backwards, catastophic",value<comparison);
            value = comparison;
        }
    }
}
