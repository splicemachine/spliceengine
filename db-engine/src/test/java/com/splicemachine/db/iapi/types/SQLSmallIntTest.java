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

import java.io.*;

/**
 *
 * Test Class for SQLSmallint
 *
 */
public class SQLSmallIntTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
            SQLSmallint integer1 = new SQLSmallint(100);
            SQLSmallint integer2 = new SQLSmallint(100);
            Assert.assertEquals("Integer Add Fails", 200, integer1.plus(integer1, integer2, null).getInt(),0);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
                SQLSmallint integer1 = new SQLSmallint(200);
                SQLSmallint integer2 = new SQLSmallint(100);
            Assert.assertEquals("Integer subtract Fails",100,integer1.minus(integer1, integer2, null).getInt(),0);
        }
        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
            SQLSmallint integer1 = new SQLSmallint(Integer.MAX_VALUE);
            SQLSmallint integer2 = new SQLSmallint(1);
            integer1.plus(integer1,integer2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLSmallint integer1 = new SQLSmallint(Integer.MIN_VALUE);
                SQLSmallint integer2 = new SQLSmallint(1);
                integer1.minus(integer1, integer2, null);
        }
    
        @Test
        public void testColumnStatistics() throws Exception {
                SQLSmallint value1 = new SQLSmallint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLSmallint SQLSmallint;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                SQLSmallint = new SQLSmallint();
                        else if (i>=1000 && i< 2000)
                                SQLSmallint = new SQLSmallint(1000+i%20);
                        else
                                SQLSmallint = new SQLSmallint(i);
                        stats.update(SQLSmallint);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLSmallint(10000),stats.maxValue());
                Assert.assertEquals(new SQLSmallint(1),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLSmallint()));
                Assert.assertEquals(55,stats.selectivity(new SQLSmallint(1010)));
                Assert.assertEquals(1,stats.selectivity(new SQLSmallint(9000)));
                Assert.assertEquals(1000.0d,(double) stats.rangeSelectivity(new SQLSmallint(1000),new SQLSmallint(2000),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d,(double) stats.rangeSelectivity(new SQLSmallint(),new SQLSmallint(500),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4000.0d,(double) stats.rangeSelectivity(new SQLSmallint(5000),new SQLSmallint(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLSmallint(1)});
                Row row = execRow.getSparkRow();
                Assert.assertEquals(1,row.getShort(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLSmallint()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLSmallint value1 = new SQLSmallint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLSmallint sqlSmallint;
                sqlSmallint = new SQLSmallint(1);
                stats.update(sqlSmallint);
                sqlSmallint = new SQLSmallint(2);
                stats.update(sqlSmallint);
                sqlSmallint = new SQLSmallint(3);
                stats.update(sqlSmallint);
                for (int i = 3; i < 81920; i++) {
                        sqlSmallint = new SQLSmallint(-1);
                        stats.update(sqlSmallint);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlSmallint);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }

        class MockObjectOutput extends ObjectOutputStream {
                public MockObjectOutput() throws IOException { super(); }
                public Boolean isNull = null;
                public Integer value = null;
                @Override
                public void writeBoolean(boolean bool) { isNull = bool; }
                @Override
                public void writeShort(int val) { value = val; }
        }

        class MockObjectInput extends ObjectInputStream {
                public MockObjectInput() throws IOException { super(); }
                public Boolean isNull = null;
                public Short value = null;
                @Override
                public boolean readBoolean() { return isNull; }
                @Override
                public short readShort() { return value; }
        }
        
        @Test
        public void writeExternal() throws StandardException, IOException {
                SQLSmallint s = new SQLSmallint(42);
                MockObjectOutput moo = new MockObjectOutput();
                s.writeExternal(moo);
                Assert.assertFalse("Shouldn't be null", moo.isNull);
                Assert.assertTrue("Unexpected value", moo.value == 42);
        }

        @Test
        public void readExternal() throws IOException {
                SQLSmallint s = new SQLSmallint();
                MockObjectInput moi = new MockObjectInput();
                moi.isNull = false;
                moi.value = 42;
                s.readExternal(moi);
                Assert.assertFalse("Shouldn't be null", s.isNull());
                Assert.assertTrue("Unexpected value", s.getShort() == 42);
        }

        @Test
        public void writeExternalNull() throws IOException {
                SQLSmallint s = new SQLSmallint();
                MockObjectOutput moo = new MockObjectOutput();
                s.writeExternal(moo);
                Assert.assertTrue("Should be null", moo.isNull);
        }

        @Test
        public void readExternalNull() throws IOException {
                SQLSmallint s = new SQLSmallint();
                MockObjectInput moi = new MockObjectInput();
                moi.isNull = true;
                moi.value = 0;
                s.readExternal(moi);
                Assert.assertTrue("Should be null", s.isNull());
        }
}
