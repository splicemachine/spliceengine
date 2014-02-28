package com.splicemachine.si.api;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dgomezferro on 2/28/14.
 */
public class TransactionStatusTest {
    @Test
    public void committingContract() {
        Assert.assertTrue(TransactionStatus.COMMITTING.isActive());
        Assert.assertTrue(TransactionStatus.COMMITTING.isCommitting());

        Assert.assertFalse(TransactionStatus.COMMITTING.isFinished());
        Assert.assertFalse(TransactionStatus.COMMITTING.isCommitted());
    }

    @Test
    public void activeContract() {
        Assert.assertTrue(TransactionStatus.ACTIVE.isActive());

        Assert.assertFalse(TransactionStatus.ACTIVE.isCommitting());
        Assert.assertFalse(TransactionStatus.ACTIVE.isFinished());
        Assert.assertFalse(TransactionStatus.ACTIVE.isCommitted());
    }

    @Test
    public void committedContract() {
        Assert.assertTrue(TransactionStatus.COMMITTED.isCommitted());
        Assert.assertTrue(TransactionStatus.COMMITTED.isFinished());

        Assert.assertFalse(TransactionStatus.COMMITTED.isActive());
        Assert.assertFalse(TransactionStatus.COMMITTED.isCommitting());
    }

    @Test
    public void errorContract() {
        Assert.assertTrue(TransactionStatus.ERROR.isFinished());

        Assert.assertFalse(TransactionStatus.ERROR.isCommitted());
        Assert.assertFalse(TransactionStatus.ERROR.isActive());
        Assert.assertFalse(TransactionStatus.ERROR.isCommitting());
    }

    @Test
    public void rolledbackContract() {
        Assert.assertTrue(TransactionStatus.ROLLED_BACK.isFinished());

        Assert.assertFalse(TransactionStatus.ROLLED_BACK.isCommitted());
        Assert.assertFalse(TransactionStatus.ROLLED_BACK.isActive());
        Assert.assertFalse(TransactionStatus.ROLLED_BACK.isCommitting());
    }
}
