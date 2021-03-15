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
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.GregorianCalendar;

/**
 *
 * Test Class for SQLTimestamp
 *
 */
public class SQLTimestampTest extends SQLDataValueDescriptorTest {

        @Test
        public void testColumnStatistics() throws Exception {

                SQLTimestamp value1 = new SQLTimestamp();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLTimestamp sqlTimestamp;

                for (int i = 10; i < 30; i++) {
                        sqlTimestamp = new SQLTimestamp();
                        if (i<20 || i >= 25) {
                                stats.update(new SQLTimestamp(Timestamp.valueOf("1962-09-" + i +" 03:23:34.234")));
                        } else {
                                stats.update(sqlTimestamp);
                        }
                }
                stats = serde(stats);
                Assert.assertEquals(5,stats.nullCount());
                Assert.assertEquals(15,stats.notNullCount());
                Assert.assertEquals(20,stats.totalCount());
                Assert.assertEquals(new SQLTimestamp(Timestamp.valueOf("1962-09-29 03:23:34.234")),stats.maxValue());
                Assert.assertEquals(new SQLTimestamp(Timestamp.valueOf("1962-09-10 03:23:34.234")),stats.minValue());
                Assert.assertEquals(5,stats.selectivity(null));
                Assert.assertEquals(5,stats.selectivity(new SQLTimestamp()));
                Assert.assertEquals(1,stats.selectivity(new SQLTimestamp(Timestamp.valueOf("1962-09-29 03:23:34.234"))));
                Assert.assertEquals(1,stats.selectivity(new SQLTimestamp(Timestamp.valueOf("1962-09-10 03:23:34.234"))));
                Assert.assertEquals(9.0d,(double) stats.rangeSelectivity(new SQLTimestamp(Timestamp.valueOf("1962-09-13 03:23:34.234")),new SQLTimestamp(Timestamp.valueOf("1962-09-27 03:23:34.234")),true,false),2);
                Assert.assertEquals(3.0d,(double) stats.rangeSelectivity(new SQLTimestamp(),new SQLTimestamp(Timestamp.valueOf("1962-09-13 03:23:34.234")),true,false),2);
                Assert.assertEquals(12.0d,(double) stats.rangeSelectivity(new SQLTimestamp(Timestamp.valueOf("1962-09-13 03:23:34.234")),new SQLTimestamp(),true,false),2);
        }


        @Test
        public void testUnion() throws StandardException {
                SQLInteger integer = new SQLInteger(2);
                ItemsUnion quantilesSketchUnion = ItemsUnion.getInstance(integer);
                 SQLInteger integer2 = new SQLInteger(2);
                ItemsSketch itemsSketch = integer.getQuantilesSketch();
                itemsSketch.update(integer2);
                quantilesSketchUnion.update(itemsSketch);
        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);
                execRow.setRowArray(new DataValueDescriptor[]{new SQLTimestamp(new DateTime(System.currentTimeMillis()))});
                Row row = execRow.getSparkRow();
                Assert.assertEquals( execRow.getColumn(1).getTimestamp(null),row.getTimestamp(0));
                ValueRow execRow2 = new ValueRow(1);
                execRow2.setRowArray(new DataValueDescriptor[]{new SQLTimestamp()});
                execRow2.getColumn(1).setSparkObject(row.get(0));
                Assert.assertEquals("ExecRow Mismatch",execRow,execRow2);
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value  */
                SQLTimestamp value1 = new SQLTimestamp();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLTimestamp sqlTimestamp;

                for (int i = 10; i < 30; i++) {
                        sqlTimestamp = new SQLTimestamp();
                        if (i<20 || i >= 25) {
                                stats.update(new SQLTimestamp(Timestamp.valueOf("1962-09-" + i +" 03:23:34.234")));
                        } else {
                                stats.update(sqlTimestamp);
                        }
                }

                sqlTimestamp = new SQLTimestamp(Timestamp.valueOf("2017-12-01 03:23:34.234"));
                stats.update(sqlTimestamp);
                sqlTimestamp = new SQLTimestamp(Timestamp.valueOf("2017-12-02 03:23:34.234"));
                stats.update(sqlTimestamp);
                sqlTimestamp = new SQLTimestamp(Timestamp.valueOf("2017-12-03 03:23:34.234"));
                stats.update(sqlTimestamp);
                for (int i = 3; i < 81920; i++) {
                        sqlTimestamp = new SQLTimestamp(Timestamp.valueOf("1970-01-01 00:00:00.000"));
                        stats.update(sqlTimestamp);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlTimestamp);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }
}
