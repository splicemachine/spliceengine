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
 * Test Class for SQLBoolean
 *
 */
public class SQLBooleanTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLBoolean value = new SQLBoolean(true);
                SQLBoolean valueA = new SQLBoolean();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertEquals("SerdeIncorrect",true,row.getBoolean(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",true,valueA.getBoolean());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLBoolean value = new SQLBoolean();
                SQLBoolean valueA = new SQLBoolean();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLBoolean value1 = new SQLBoolean(false);
                SQLBoolean value2 = new SQLBoolean(true);
                SQLBoolean value1a = new SQLBoolean();
                SQLBoolean value2a = new SQLBoolean();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getBoolean(),value1a.getBoolean());
                Assert.assertEquals("2 incorrect",value2.getBoolean(),value2a.getBoolean());
        }

        @Test
        public void testColumnStatistics() throws Exception {
                SQLBoolean value1 = new SQLBoolean();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLBoolean sqlBoolean;
                for (int i = 1; i<= 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlBoolean = new SQLBoolean();
                        else if (i>=1000 && i< 2000)
                                sqlBoolean = new SQLBoolean(false);
                        else
                                sqlBoolean = new SQLBoolean(i%2==0?true:false);
                        stats.update(sqlBoolean);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLBoolean(true),stats.maxValue());
                Assert.assertEquals(new SQLBoolean(false),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLBoolean()));
                Assert.assertEquals(4000,stats.selectivity(new SQLBoolean(true)));
                Assert.assertEquals(5000,stats.selectivity(new SQLBoolean(false)));
                Assert.assertEquals(5000.0d,(double) stats.rangeSelectivity(new SQLBoolean(false),new SQLBoolean(true),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(5000.0d,(double) stats.rangeSelectivity(new SQLBoolean(),new SQLBoolean(true),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(9000.0d,(double) stats.rangeSelectivity(new SQLBoolean(false),new SQLBoolean(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }
}
