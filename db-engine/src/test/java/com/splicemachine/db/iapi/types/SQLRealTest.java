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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 * Test Class for SQLDouble
 *
 */
public class SQLRealTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
            SQLReal float1 = new SQLReal(100.0f);
            SQLReal float2 = new SQLReal(100.0f);
            Assert.assertEquals("Integer Add Fails", 200.0f, float1.plus(float1, float2, null).getFloat(),0.0f);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
            SQLReal float1 = new SQLReal(200.0f);
            SQLReal float2 = new SQLReal(100.0f);
            Assert.assertEquals("Integer subtract Fails",100.0f,float1.minus(float1, float2, null).getFloat(),0.0f);
        }

        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
                SQLReal float1 = new SQLReal(Float.MAX_VALUE);
                SQLReal float2 = new SQLReal(1.0f);
                float1.plus(float1,float2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLReal float1 = new SQLReal(Float.MIN_VALUE);
                SQLReal float2 = new SQLReal(1.0f);
                float1.minus(float1, float2, null);
        }
    
        @Test
        public void testColumnStatistics() throws Exception {

                SQLReal value1 = new SQLReal();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLReal SQLReal;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                SQLReal = new SQLReal();
                        else if (i>=1000 && i< 2000)
                                SQLReal = new SQLReal(1000+i%20);
                        else
                                SQLReal = new SQLReal(i);
                        stats.update(SQLReal);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLReal(10000),stats.maxValue());
                Assert.assertEquals(new SQLReal(1),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLReal()));
                Assert.assertEquals(55,stats.selectivity(new SQLReal(1010)));
                Assert.assertEquals(1,stats.selectivity(new SQLReal(9000)));
                Assert.assertEquals(1000.0d,(double) stats.rangeSelectivity(new SQLReal(1000),new SQLReal(2000),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d,(double) stats.rangeSelectivity(new SQLReal(),new SQLReal(500),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4000.0d,(double) stats.rangeSelectivity(new SQLReal(5000),new SQLReal(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLReal(1234)});
                Row row = execRow.getSparkRow();
                Assert.assertEquals(1234f,row.getFloat(0),0.0f);
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLReal()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLReal value1 = new SQLReal();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLReal sqlReal;
                sqlReal = new SQLReal(1.0f);
                stats.update(sqlReal);
                sqlReal = new SQLReal(2.0f);
                stats.update(sqlReal);
                sqlReal = new SQLReal(3.0f);
                stats.update(sqlReal);
                for (int i = 3; i < 81920; i++) {
                        sqlReal = new SQLReal(-1.0f);
                        stats.update(sqlReal);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlReal);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }
}
