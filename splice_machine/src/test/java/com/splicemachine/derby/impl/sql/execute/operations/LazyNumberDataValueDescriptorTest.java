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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.sql.execute.dvd.LazyDouble;
import com.splicemachine.derby.impl.sql.execute.dvd.LazyNumberDataValueDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ArchitectureIndependent.class)
public class LazyNumberDataValueDescriptorTest {

    @Test
    public void testSetDoubleValue() throws StandardException {

        LazyNumberDataValueDescriptor lndvd = new LazyDouble();
        Assert.assertTrue(lndvd.isNull());

        lndvd.setValue(new Double(3.3));

        Assert.assertEquals(3.3, lndvd.getDouble(), 0.0);

        Assert.assertFalse(lndvd.isNull());

    }
}
