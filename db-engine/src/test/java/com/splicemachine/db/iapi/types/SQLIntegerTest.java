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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test Class for SQLInteger
 *
 */
public class SQLIntegerTest {

        @Test
        public void addTwo() throws StandardException {
            SQLInteger integer1 = new SQLInteger(100);
            SQLInteger integer2 = new SQLInteger(100);
            Assert.assertEquals("Integer Add Fails", 200, integer1.plus(integer1, integer2, null).getInt(),0);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
                SQLInteger integer1 = new SQLInteger(200);
                SQLInteger integer2 = new SQLInteger(100);
            Assert.assertEquals("Integer subtract Fails",100,integer1.minus(integer1, integer2, null).getInt(),0);
        }
        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
            SQLInteger integer1 = new SQLInteger(Integer.MAX_VALUE);
            SQLInteger integer2 = new SQLInteger(1);
            integer1.plus(integer1,integer2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLInteger integer1 = new SQLInteger(Integer.MIN_VALUE);
                SQLInteger integer2 = new SQLInteger(1);
                integer1.minus(integer1, integer2, null);
        }
    
        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLInteger value = new SQLInteger(100);
                SQLInteger valueA = new SQLInteger();
                value.write(writer, 0);
                Assert.assertEquals("SerdeIncorrect",100,row.getInt(0),0);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",100,valueA.getInt(),0);
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLInteger value = new SQLInteger();
                SQLInteger valueA = new SQLInteger();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLInteger value1 = new SQLInteger(100);
                SQLInteger value2 = new SQLInteger(200);
                SQLInteger value1a = new SQLInteger();
                SQLInteger value2a = new SQLInteger();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getInt(),value1a.getInt(),0);
                Assert.assertEquals("2 incorrect",value2.getInt(),value2a.getInt(),0);
        }

        @Test
        public void rowValueToDVDValue() throws Exception {
                SQLInteger int1 = new SQLInteger(10);
                SQLInteger int2 = new SQLInteger();
                ExecRow execRow = new ValueRow(2);
                execRow.setColumn(1,int1);
                execRow.setColumn(2,int2);
                Assert.assertEquals(10,((Row)execRow).getInt(0));
                Assert.assertTrue(((Row)execRow).isNullAt(1));
                Row sparkRow = execRow.getSparkRow();
                GenericRowWithSchema row = new GenericRowWithSchema(new Object[]{10,null}, execRow.schema());
                Assert.assertEquals(row.getInt(0),10);
                Assert.assertTrue(row.isNullAt(1));
        }


}
