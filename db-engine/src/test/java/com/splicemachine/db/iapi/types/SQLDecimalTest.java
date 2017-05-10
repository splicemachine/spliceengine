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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.Decimal;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

/**
 *
 * Test Class for SQLDecimal
 *
 */
public class SQLDecimalTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
            SQLDecimal decimal1 = new SQLDecimal(new BigDecimal(100.0d));
            SQLDecimal decimal2 = new SQLDecimal(new BigDecimal(100.0d));
            Assert.assertEquals("Integer Add Fails", new BigDecimal(200.0d), decimal1.plus(decimal1, decimal2, null).getBigDecimal());
        }
    
        @Test
        public void subtractTwo() throws StandardException {
            SQLDecimal decimal1 = new SQLDecimal(new BigDecimal(200.0d));
            SQLDecimal decimal2 = new SQLDecimal(new BigDecimal(100.0d));
            Assert.assertEquals("Integer subtract Fails",new BigDecimal(100.0d),decimal1.minus(decimal1, decimal2, null).getBigDecimal());
        }
        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLDecimal value = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal valueA = new SQLDecimal(null,value.precision,value.getScale());
                writer.reset();
                value.write(writer, 0);
                Decimal sparkDecimal = row.getDecimal(0,value.getDecimalValuePrecision(),value.getDecimalValueScale());
                scala.math.BigDecimal foo = sparkDecimal.toBigDecimal();
                BigDecimal decimal = sparkDecimal.toBigDecimal().bigDecimal();
                Assert.assertEquals("SerdeIncorrect",new BigDecimal("100"),sparkDecimal.toBigDecimal().bigDecimal());
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",new BigDecimal("100"),valueA.getBigDecimal());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLDecimal value = new SQLDecimal();
                SQLDecimal valueA = new SQLDecimal();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
        // TODO
        @Test
        @Ignore
        public void serdeKeyData() throws Exception {
                SQLDecimal value1 = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal value2 = new SQLDecimal(new BigDecimal(200.0d));
                SQLDecimal value1a = new SQLDecimal();
                value1a.setPrecision(value1.getPrecision());
                value1a.setScale(value1.getScale());
                SQLDecimal value2a = new SQLDecimal();
                value2a.setPrecision(value2.getPrecision());
                value2a.setScale(value2.getScale());
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getBigDecimal().doubleValue(),value1a.getBigDecimal().doubleValue(),0.1d);
                Assert.assertEquals("2 incorrect",value2.getBigDecimal().doubleValue(),value2a.getBigDecimal().doubleValue(),0.1d);
        }

        @Test
        public void testColumnStatistics() throws Exception {
                SQLDecimal value1 = new SQLDecimal();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLDecimal SQLDecimal;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                SQLDecimal = new SQLDecimal();
                        else if (i>=1000 && i< 2000)
                                SQLDecimal = new SQLDecimal(""+(1000+i%20));
                        else
                                SQLDecimal = new SQLDecimal(""+i);
                        stats.update(SQLDecimal);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLDecimal(""+10000),stats.maxValue());
                Assert.assertEquals(new SQLDecimal(""+1),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLDecimal()));
                Assert.assertEquals(51,stats.selectivity(new SQLDecimal(""+1010)));
                Assert.assertEquals(1,stats.selectivity(new SQLDecimal(""+9000)));
                Assert.assertEquals(1000.0d,(double) stats.rangeSelectivity(new SQLDecimal(""+1000),new SQLDecimal(""+2000),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d,(double) stats.rangeSelectivity(new SQLDecimal(),new SQLDecimal(""+500),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4000.0d,(double) stats.rangeSelectivity(new SQLDecimal(""+5000),new SQLDecimal(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }


}
