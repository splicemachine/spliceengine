/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
public class PrimaryKeyStatisticsImplTest extends AbstractKeyStatisticsImplTest {

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
