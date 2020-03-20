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
 * Test Class for SQLChar
 *
 */
public class SQLVarcharTest extends SQLDataValueDescriptorTest {

        @Test
        public void testColumnStatistics() throws Exception {

                SQLVarchar value1 = new SQLVarchar();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLVarchar sqlVarchar;

                for (int i = 0; i < 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlVarchar = new SQLVarchar();
                        else
                                sqlVarchar = new SQLVarchar(new char[]{(char) ('A' + (i%26))});
                        stats.update(sqlVarchar);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLVarchar(new char[]{'Z'}),stats.maxValue());
                Assert.assertEquals(new SQLVarchar(new char[]{'A'}),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLVarchar()));
                Assert.assertEquals(347,stats.selectivity(new SQLVarchar(new char[]{'A'})));
                Assert.assertEquals(347,stats.selectivity(new SQLVarchar(new char[]{'F'})));

                double range = stats.rangeSelectivity(new SQLVarchar(new char[]{'C'}),new SQLVarchar(new char[]{'G'}),true,false);
                //avg rows per value is 347/346
                Assert.assertTrue((range == 1388.0d || range == 1404.0d));

                range = stats.rangeSelectivity(new SQLVarchar(),new SQLVarchar(new char[]{'C'}),true,false);
                Assert.assertTrue((range == 702.0d || range == 694.0d));

                Assert.assertEquals(2392.0d,(double) stats.rangeSelectivity(new SQLVarchar(new char[]{'T'}),new SQLVarchar(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLVarchar("1234")});
                Row row = execRow.getSparkRow();
                Assert.assertEquals("1234",row.getString(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use the default value 'ZZZZ' */
                SQLVarchar value1 = new SQLVarchar();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLVarchar sqlVarchar;
                sqlVarchar = new SQLVarchar("aaaa");
                stats.update(sqlVarchar);
                sqlVarchar = new SQLVarchar("bbbb");
                stats.update(sqlVarchar);
                sqlVarchar = new SQLVarchar("cccc");
                stats.update(sqlVarchar);
                for (int i = 3; i < 81920; i++) {
                        sqlVarchar = new SQLVarchar("ZZZZ");
                        stats.update(sqlVarchar);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlVarchar);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }
}
