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
 * Test Class for SQLTinyint
 *
 */
public class SQLTinyIntTest extends SQLDataValueDescriptorTest {

        @Test
        public void testColumnStatistics() throws Exception {

                SQLTinyint value1 = new SQLTinyint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLTinyint sqlTinyint;

                for (int i = 0; i < 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlTinyint = new SQLTinyint();
                        else
                                sqlTinyint = new SQLTinyint((byte) ('A' + (i%26)));
                        stats.update(sqlTinyint);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLTinyint((byte)'Z'),stats.maxValue());
                Assert.assertEquals(new SQLTinyint((byte)'A'),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLTinyint()));
                Assert.assertEquals(347,stats.selectivity(new SQLTinyint((byte)'A')));
                Assert.assertEquals(347,stats.selectivity(new SQLTinyint((byte)'F')));
                Assert.assertEquals(1404.0d,(double) stats.rangeSelectivity(new SQLTinyint((byte)'C'),new SQLTinyint((byte)'G'),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(700.0d,(double) stats.rangeSelectivity(new SQLTinyint(),new SQLTinyint((byte)'C'),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(2392.0d,(double) stats.rangeSelectivity(new SQLTinyint((byte)'T'),new SQLTinyint(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLTinyint((byte) 1)});
                Row row = execRow.getSparkRow();
                Assert.assertEquals( (byte) 1,row.getByte(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLTinyint()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLTinyint value1 = new SQLTinyint();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLTinyint sqlTinyint;
                sqlTinyint = new SQLTinyint(Byte.valueOf("1"));
                stats.update(sqlTinyint);
                sqlTinyint = new SQLTinyint(Byte.valueOf("2"));
                stats.update(sqlTinyint);
                sqlTinyint = new SQLTinyint(Byte.valueOf("3"));
                stats.update(sqlTinyint);
                for (int i = 3; i < 81920; i++) {
                        sqlTinyint = new SQLTinyint(Byte.valueOf("-1"));
                        stats.update(sqlTinyint);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlTinyint);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }
}
