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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * Test Class for SQLDouble
 *
 */
public class SQLDoubleTest {

        @Test
        public void addTwo() throws StandardException {
            SQLDouble double1 = new SQLDouble(100.0d);
            SQLDouble double2 = new SQLDouble(100.0d);
            Assert.assertEquals("Integer Add Fails", 200.0d, double1.plus(double1, double2, null).getDouble(),0.0d);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
            SQLDouble double1 = new SQLDouble(200.0d);
            SQLDouble double2 = new SQLDouble(100.0d);
            Assert.assertEquals("Integer subtract Fails",100.0d,double1.minus(double1, double2, null).getDouble(),0.0d);
        }
        @Ignore
        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
            SQLDouble double1 = new SQLDouble(Double.MAX_VALUE);
            SQLDouble double2 = new SQLDouble(1.0d);
            double1.plus(double1,double2,null).getDouble();
        }

        @Ignore
        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLDouble double1 = new SQLDouble(Double.MIN_VALUE);
                SQLDouble double2 = new SQLDouble(1.0d);
                double1.minus(double1, double2, null).getDouble();
        }
    
        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLDouble value = new SQLDouble(100.0d);
                SQLDouble valueA = new SQLDouble();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertEquals("SerdeIncorrect",100.0d,row.getDouble(0),0.0d);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",100,valueA.getDouble(),0.0d);
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLDouble value = new SQLDouble();
                SQLDouble valueA = new SQLDouble();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
                @Test
        public void serdeKeyData() throws Exception {
                SQLDouble value1 = new SQLDouble(100.0d);
                SQLDouble value2 = new SQLDouble(200.0d);
                SQLDouble value1a = new SQLDouble();
                SQLDouble value2a = new SQLDouble();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getDouble(),value1a.getDouble(),0.0d);
                Assert.assertEquals("2 incorrect",value2.getDouble(),value2a.getDouble(),0.0d);
            }
    
}
