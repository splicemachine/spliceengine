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
 * Test Class for SQLLongint
 *
 */
public class SQLLongIntTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
            SQLLongint long1 = new SQLLongint(100l);
            SQLLongint long2 = new SQLLongint(100l);
            Assert.assertEquals("Integer Add Fails", 200l, long1.plus(long1, long2, null).getLong(),0l);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
            SQLLongint long1 = new SQLLongint(200l);
            SQLLongint long2 = new SQLLongint(100l);
            Assert.assertEquals("Integer subtract Fails",100l,long1.minus(long1, long2, null).getLong(),0l);
        }
        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
            SQLLongint long1 = new SQLLongint(Long.MAX_VALUE);
            SQLLongint long2 = new SQLLongint(1);
            long1.plus(long1,long2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLLongint long1 = new SQLLongint(Long.MIN_VALUE);
                SQLLongint long2 = new SQLLongint(1);
                long1.minus(long1, long2, null);
        }
    
        @Test
        public void testColumnStatistics() throws Exception {
                SQLLongint value1 = new SQLLongint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLLongint SQLLongint;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                SQLLongint = new SQLLongint();
                        else if (i>=1000 && i< 2000)
                                SQLLongint = new SQLLongint(1000+i%20);
                        else
                                SQLLongint = new SQLLongint(i);
                        stats.update(SQLLongint);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLLongint(10000),stats.maxValue());
                Assert.assertEquals(new SQLLongint(1),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLLongint()));
                Assert.assertEquals(55,stats.selectivity(new SQLLongint(1010)));
                Assert.assertEquals(1,stats.selectivity(new SQLLongint(9000)));
                Assert.assertEquals(1000.0d,(double) stats.rangeSelectivity(new SQLLongint(1000),new SQLLongint(2000),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d,(double) stats.rangeSelectivity(new SQLLongint(),new SQLLongint(500),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4000.0d,(double) stats.rangeSelectivity(new SQLLongint(5000),new SQLLongint(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLLongint(1234)});
                Row row = execRow.getSparkRow();
                Assert.assertEquals(1234l,row.getLong(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLLongint()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLLongint value1 = new SQLLongint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLLongint sqlLongint;
                sqlLongint = new SQLLongint(1L);
                stats.update(sqlLongint);
                sqlLongint = new SQLLongint(2L);
                stats.update(sqlLongint);
                sqlLongint = new SQLLongint(3L);
                stats.update(sqlLongint);
                for (int i = 3; i < 81920; i++) {
                        sqlLongint = new SQLLongint(-1L);
                        stats.update(sqlLongint);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlLongint);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }

        class MockObjectOutput extends ObjectOutputStream {
                public MockObjectOutput() throws IOException { super(); }
                public Boolean isNull = null;
                public Long value = null;
                @Override
                public void writeBoolean(boolean bool) { isNull = bool; }
                @Override
                public void writeLong(long val) { value = val; }
        }

        class MockObjectInput extends ObjectInputStream {
                public MockObjectInput() throws IOException { super(); }
                public Boolean isNull = null;
                public Long value = null;
                @Override
                public boolean readBoolean() { return isNull; }
                @Override
                public long readLong() { return value; }
        }

        @Test
        public void writeExternal() throws IOException {
                SQLLongint lint = new SQLLongint(42);
                MockObjectOutput moo = new MockObjectOutput();
                lint.writeExternal(moo);
                Assert.assertFalse("Shouldn't be null", moo.isNull);
                Assert.assertTrue("Unexpected value", moo.value == 42);
        }

        @Test
        public void readExternal() throws IOException {
                SQLLongint lint = new SQLLongint();
                MockObjectInput moi = new MockObjectInput();
                moi.isNull = false;
                moi.value = 42L;
                lint.readExternal(moi);
                Assert.assertFalse("Shouldn't be null", lint.isNull());
                Assert.assertTrue("Unexpected value", lint.getLong() == 42);
        }

        @Test
        public void writeExternalNull() throws IOException {
                SQLLongint lint = new SQLLongint();
                MockObjectOutput moo = new MockObjectOutput();
                lint.writeExternal(moo);
                Assert.assertTrue("Should be null", moo.isNull);
        }

        @Test
        public void readExternalNull() throws IOException {
                SQLLongint lint = new SQLLongint();
                MockObjectInput moi = new MockObjectInput();
                moi.isNull = true;
                moi.value = 0L;
                lint.readExternal(moi);
                Assert.assertTrue("Should be null", lint.isNull());
        }
}
