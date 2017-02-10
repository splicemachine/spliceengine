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
package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.hadoop.yarn.util.Times;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

/**
 *
 * Test Class for SQLTimestamp
 *
 */
public class SQLTimestampTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                SQLTimestamp value = new SQLTimestamp(timestamp);
                SQLTimestamp valueA = new SQLTimestamp();
                writer.reset();
                value.write(writer, 0);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",timestamp.toString(),valueA.getTimestamp(new GregorianCalendar()).toString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLTimestamp value = new SQLTimestamp();
                SQLTimestamp valueA = new SQLTimestamp();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                GregorianCalendar gc = new GregorianCalendar();
                long currentTimeMillis = System.currentTimeMillis();
                SQLTimestamp value1 = new SQLTimestamp(new Timestamp(currentTimeMillis));
                SQLTimestamp value2 = new SQLTimestamp(new Timestamp(currentTimeMillis+200));
                SQLTimestamp value1a = new SQLTimestamp();
                SQLTimestamp value2a = new SQLTimestamp();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getTimestamp(gc),value1a.getTimestamp(gc));
                Assert.assertEquals("2 incorrect",value2.getTimestamp(gc),value2a.getTimestamp(gc));
        }

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

}
