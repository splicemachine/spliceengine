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
public class SQLRealTest {

        @Test
        public void addTwo() throws StandardException {
            SQLReal float1 = new SQLReal(100.0f);
            SQLReal float2 = new SQLReal(100.0f);
            Assert.assertEquals("Integer Add Fails", 200.0f, float1.plus(float1, float2, null).getFloat(),0.0f);
        }
    
        @Test
        public void subtractTwo() throws StandardException {
            SQLReal float1 = new SQLReal(200.0f);
            SQLReal float2 = new SQLReal(100.0f);
            Assert.assertEquals("Integer subtract Fails",100.0f,float1.minus(float1, float2, null).getFloat(),0.0f);
        }

        @Test(expected = StandardException.class)
        public void testPositiveOverFlow() throws StandardException {
                SQLReal float1 = new SQLReal(Float.MAX_VALUE);
                SQLReal float2 = new SQLReal(1.0f);
                float1.plus(float1,float2,null);
        }

        @Test(expected = StandardException.class)
        public void testNegativeOverFlow() throws StandardException {
                SQLReal float1 = new SQLReal(Float.MIN_VALUE);
                SQLReal float2 = new SQLReal(1.0f);
                float1.minus(float1, float2, null);
        }
    
        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLReal value = new SQLReal(100.0f);
                SQLReal valueA = new SQLReal();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertEquals("SerdeIncorrect",100.0f,row.getFloat(0),0.0f);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",100f,valueA.getFloat(),0.0d);
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLReal value = new SQLReal();
                SQLReal valueA = new SQLReal();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLReal value1 = new SQLReal(100.0f);
                SQLReal value2 = new SQLReal(200.0f);
                SQLReal value1a = new SQLReal();
                SQLReal value2a = new SQLReal();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getFloat(),value1a.getFloat(),0.0f);
                Assert.assertEquals("2 incorrect",value2.getFloat(),value2a.getFloat(),0.0f);
            }
    
}
