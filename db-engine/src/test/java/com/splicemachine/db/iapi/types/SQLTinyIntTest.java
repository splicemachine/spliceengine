/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test Class for SQLTinyint
 *
 */
public class SQLTinyIntTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLTinyint value = new SQLTinyint(Byte.valueOf("1"));
                SQLTinyint valueA = new SQLTinyint();
                value.write(writer, 0);
                Assert.assertEquals("SerdeIncorrect",(Object) Byte.valueOf("1"),(Object) row.getByte(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",(Object) Byte.valueOf("1"),(Object) valueA.getByte());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLTinyint value = new SQLTinyint();
                SQLTinyint valueA = new SQLTinyint();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLTinyint value1 = new SQLTinyint(Byte.valueOf("1"));
                SQLTinyint value2 = new SQLTinyint(Byte.valueOf("2"));
                SQLTinyint value1a = new SQLTinyint();
                SQLTinyint value2a = new SQLTinyint();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getByte(),value1a.getByte());
                Assert.assertEquals("2 incorrect",value2.getByte(),value2a.getByte());
        }

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


}
