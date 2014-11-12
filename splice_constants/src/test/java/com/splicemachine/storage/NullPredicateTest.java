package com.splicemachine.storage;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class NullPredicateTest {

    @Test
    public void testMatchesNullFilterIfMissingFalse() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(false, false, 0, false, false);

        Assert.assertTrue("Does not match null!",nullPredicate.match(0,null,0,0));
        Assert.assertTrue("Does not match empty byte[]",nullPredicate.match(0,new byte[]{},0,0));
    }

    @Test
    public void testFiltersOutNullFields() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(true, false, 0, false, false);

        Assert.assertFalse("Erroneously matches null!", nullPredicate.match(0, null, 0, 0));
        Assert.assertFalse("Erroneously matches empty byte[]", nullPredicate.match(0, new byte[]{}, 0, 0));
    }

    @Test
    public void testCanMatchFloatsFilterIfMissingFalse() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(false, false, 0, true, false);

        Assert.assertTrue("does not match null!", nullPredicate.match(0, null, 0, 0));
        Assert.assertTrue("does not match empty byte[]", nullPredicate.match(0, new byte[]{}, 0, 0));
        Assert.assertTrue("does not match incorrectly sized double!",nullPredicate.match(0,new byte[]{0,1,2,3,4,5,6,7,8},0,7));
    }

    @Test
    public void testCanMatchDoublesFilterIfMissingFalse() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(false, false, 0, false, true);

        Assert.assertTrue("does not match null!", nullPredicate.match(0, null, 0, 0));
        Assert.assertTrue("does not match empty byte[]", nullPredicate.match(0, new byte[]{}, 0, 0));
        Assert.assertTrue("does not match incorrectly sized double!",nullPredicate.match(0,new byte[]{0,1,2,3,4,5,6,7,8},0,7));
    }

    @Test
    public void testFiltersOutNullDoubleFields() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(true, false, 0, false, true);

        Assert.assertFalse("Erroneously matches null!", nullPredicate.match(0, null, 0, 0));
        Assert.assertFalse("Erroneously matches empty byte[]", nullPredicate.match(0, new byte[]{}, 0, 0));
        Assert.assertFalse("Erroneously matches incorrectly sized double!",nullPredicate.match(0,new byte[]{0,1,2,3,4,5,6,7,8},0,7));
    }

    @Test
    public void testFiltersOutNullFloatFields() throws Exception {
        NullPredicate nullPredicate = new NullPredicate(true, false, 0, true, false);

        Assert.assertFalse("Erroneously matches null!", nullPredicate.match(0, null, 0, 0));
        Assert.assertFalse("Erroneously matches empty byte[]", nullPredicate.match(0, new byte[]{}, 0, 0));
        Assert.assertFalse("Erroneously matches incorrectly sized double!",nullPredicate.match(0,new byte[]{0,1,2,3,4,5,6,7,8},0,7));
    }
}
