package com.splicemachine.si.impl;

import org.junit.Assert;
import org.junit.Test;

public class TransactionIdTest {
    @Test
    public void nestedReadOnlyIds() {
        final TransactionId id = new TransactionId(100L, true);
        Assert.assertEquals(100L, id.getId());
        Assert.assertEquals("100.IRO", id.getTransactionIdString());
        final TransactionId id2 = new TransactionId("200.IRO");
        Assert.assertEquals(200L, id2.getId());
        Assert.assertEquals("200.IRO", id2.getTransactionIdString());
    }

}
