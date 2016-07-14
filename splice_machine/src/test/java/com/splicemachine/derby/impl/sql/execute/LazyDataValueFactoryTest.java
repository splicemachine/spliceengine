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

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.dvd.LazyDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.dvd.LazyStringDataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ArchitectureIndependent.class)
public class LazyDataValueFactoryTest {

    private final DataValueFactory lfac = new LazyDataValueFactory();

    @Test
    public void testGetVarcharDataValue() throws Exception {
        String value = "foo";

        LazyDataValueDescriptor dvd = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);
        LazyDataValueDescriptor dvd2 = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);


        Assert.assertEquals(value, dvd.getString());
        Assert.assertEquals(value, dvd2.getString());

    }

    @Test
    public void testGetVarcharDataValueWithPrevious() throws Exception {

        LazyStringDataValueDescriptor dvd = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");

        Assert.assertEquals("foo", dvd.getString());

        LazyStringDataValueDescriptor dvd2 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("bar", dvd);

        Assert.assertEquals(dvd.getString(), "bar");
        Assert.assertEquals(dvd2.getString(), "bar");

        Assert.assertTrue(dvd == dvd2);
    }

}
