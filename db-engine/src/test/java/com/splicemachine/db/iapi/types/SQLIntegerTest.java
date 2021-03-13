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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 * Test Class for SQLInteger
 *
 */
public class SQLIntegerTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
            SQLInteger integer1 = new SQLInteger(100);
            SQLInteger integer2 = new SQLInteger(100);
            Assert.assertEquals("Integer Add Fails", 200, integer1.plus(integer1, integer2, null).getInt(),0);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
                SQLInteger integer1 = new SQLInteger(200);
                SQLInteger integer2 = new SQLInteger(100);
            Assert.assertEquals("Integer subtract Fails",100,integer1.minus(integer1, integer2, null).getInt(),0);
        }
        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
            SQLInteger integer1 = new SQLInteger(Integer.MAX_VALUE);
            SQLInteger integer2 = new SQLInteger(1);
            integer1.plus(integer1,integer2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLInteger integer1 = new SQLInteger(Integer.MIN_VALUE);
                SQLInteger integer2 = new SQLInteger(1);
                integer1.minus(integer1, integer2, null);
        }
    
        @Test
        public void rowValueToDVDValue() throws Exception {
                SQLInteger int1 = new SQLInteger(10);
                SQLInteger int2 = new SQLInteger();
                ExecRow execRow = new ValueRow(2);
                execRow.setColumn(1, int1);
                execRow.setColumn(2, int2);
                Assert.assertEquals(10, ((Row) execRow).getInt(0));
                Assert.assertTrue(((Row) execRow).isNullAt(1));
                Row sparkRow = execRow.getSparkRow();
                Assert.assertEquals(sparkRow.getInt(0), 10);
                Assert.assertTrue(sparkRow.isNullAt(1));
        }
        @Test
        public void testColumnStatistics() throws Exception {
                SQLInteger value1 = new SQLInteger();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLInteger sqlInteger;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlInteger = new SQLInteger();
                        else if (i>=1000 && i< 2000)
                                sqlInteger = new SQLInteger(1000+i%20);
                        else
                                sqlInteger = new SQLInteger(i);
                        stats.update(sqlInteger);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLInteger(10000),stats.maxValue());
                Assert.assertEquals(new SQLInteger(1),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLInteger()));
                Assert.assertEquals(55,stats.selectivity(new SQLInteger(1010)));
                Assert.assertEquals(1,stats.selectivity(new SQLInteger(9000)));
                Assert.assertEquals(1000.0d,(double) stats.rangeSelectivity(new SQLInteger(1000),new SQLInteger(2000),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d,(double) stats.rangeSelectivity(new SQLInteger(),new SQLInteger(500),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4000.0d,(double) stats.rangeSelectivity(new SQLInteger(5000),new SQLInteger(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(1234)});
                Row row = execRow.getSparkRow();
                Assert.assertEquals(1234,row.getInt(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLInteger()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLInteger value1 = new SQLInteger();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLInteger sqlInteger;
                sqlInteger = new SQLInteger(1);
                stats.update(sqlInteger);
                sqlInteger = new SQLInteger(2);
                stats.update(sqlInteger);
                sqlInteger = new SQLInteger(3);
                stats.update(sqlInteger);
                for (int i = 3; i < 81920; i++) {
                        sqlInteger = new SQLInteger(-1);
                        stats.update(sqlInteger);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlInteger);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }
}
