package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the SimpleBooleanFrequencyCounter. Since it's super-simple in implementation,
 * we don't need a whole lot of tests here, but it's useful to have a "standard suite" of tests
 * for all types.
 *
 * @author Scott Fines
 *         Date: 12/7/14
 */
public class FixedSimpleBooleanFrequencyCounterTest {

    @Test
    public void testCanMergeFrequencies() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(true);
        BooleanFrequentElements elem1 = counter.frequencies();
        BooleanFrequencyCounter c2 = new SimpleBooleanFrequencyCounter();
        c2.update(false,4);
        BooleanFrequentElements elem2 = c2.frequencies();

        BooleanFrequentElements merged = elem1.merge(elem2);
        assertCorrect(1,4,merged);
    }

    @Test
    public void testWorksWithOnlyTrue() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(true);

        checkCounter(1,0,counter);
    }

    @Test
    public void testWorksWithOnlyFalse() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(false);

        checkCounter(0,1,counter);
    }

    @Test
    public void testWorksWithBothTrueAndFalse() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(false);
        counter.update(true);

        checkCounter(1,1,counter);
    }

    @Test
    public void testWorksWithOnlyTrueMultipleCount() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(true,10);

        checkCounter(10,0,counter);
    }

    @Test
    public void testWorksWithOnlyFalseMultipleCount() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(false,10);

        checkCounter(0,10,counter);
    }

    @Test
    public void testWorksWithBothTrueAndFalseMultipleCount() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(false,10);
        counter.update(true,12);

        checkCounter(12,10,counter);
    }

    /*Object version method tests*/
    @Test
    public void testWorksWithOnlyTrueBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.TRUE);

        checkCounter(1,0,counter);
    }

    @Test
    public void testWorksWithOnlyFalseBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.FALSE);

        checkCounter(0,1,counter);
    }

    @Test
    public void testWorksWithBothTrueAndFalseBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.FALSE);
        counter.update(Boolean.TRUE);

        checkCounter(1,1,counter);
    }

    @Test
    public void testWorksWithOnlyTrueMultipleCountBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.TRUE,10);

        checkCounter(10,0,counter);
    }

    @Test
    public void testWorksWithOnlyFalseMultipleCountBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.FALSE,10);

        checkCounter(0,10,counter);
    }

    @Test
    public void testWorksWithBothTrueAndFalseMultipleCountBooleanObject() throws Exception {
        BooleanFrequencyCounter counter = new SimpleBooleanFrequencyCounter();
        counter.update(Boolean.FALSE,10);
        counter.update(Boolean.TRUE,12);

        checkCounter(12,10,counter);
    }

    /**********************************************************************************/
    /*private helper methods*/
    private void checkCounter(long trueCount, long falseCount,BooleanFrequencyCounter counter) {
        BooleanFrequentElements boolElements = counter.frequencies();
        assertCorrect(trueCount, falseCount, boolElements);
    }

    private void assertCorrect(long trueCount, long falseCount, BooleanFrequentElements boolElements) {
        Assert.assertEquals("Incorrect value for true!", trueCount, boolElements.equalsTrue().count());
        Assert.assertEquals("Incorrect value for false!", falseCount,boolElements.equalsFalse().count());
    }
}
