/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
