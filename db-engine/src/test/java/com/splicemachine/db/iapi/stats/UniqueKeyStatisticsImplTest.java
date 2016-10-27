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
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by jleach on 8/5/16.
 */
public class UniqueKeyStatisticsImplTest extends AbstractKeyStatisticsImplTest {

    @BeforeClass
    public static void beforeClass() throws StandardException {
        init();
    }

    @Test
    public void testNullCount() throws StandardException {
        Assert.assertEquals(0, impl.nullCount());
    }

    @Test
    public void testNotNullCount() throws StandardException {
        Assert.assertEquals(10000,impl.notNullCount());
    }

    @Test
    public void testTotalCount() throws StandardException {
        Assert.assertEquals(10000,impl.totalCount());
    }

    @Test
    public void testMaxValue() throws StandardException {
        Assert.assertEquals(maxRow,impl.maxValue());
    }

    @Test
    public void testMinValue() throws StandardException {
        Assert.assertEquals(minRow,impl.minValue());
    }

    @Test
    public void testNullSelectivity() throws StandardException {
        Assert.assertEquals(1,impl.selectivity(null));
    }

    @Test
    public void testEmptySelectivity() throws StandardException {
        Assert.assertEquals(1,impl.selectivity(new ValueRow(3)));
    }

    @Test
    public void testIndividualSelectivity() throws StandardException {
        Assert.assertEquals(1,impl.selectivity(minRow));
    }

    @Test
    public void testRangeSelectivity() throws StandardException {
        Assert.assertEquals(4000.0d,(double) impl.rangeSelectivity(row2000,row6000,true,false),50.0d);
    }

    @Test
    public void testNullBeginSelectivity() throws StandardException {
        Assert.assertEquals(6000.0d,(double) impl.rangeSelectivity(null,row6000,true,false),50.0d);
    }

    @Test
    public void testNullEndSelectivity() throws StandardException {
        Assert.assertEquals(8000.0d,(double) impl.rangeSelectivity(row2000,null,true,false),50.0d);
    }

    @Test
    public void testFullScanSelectivity() throws StandardException {
        Assert.assertEquals(10000.0d,(double) impl.rangeSelectivity(null,null,true,false),50.0d);
    }

}
