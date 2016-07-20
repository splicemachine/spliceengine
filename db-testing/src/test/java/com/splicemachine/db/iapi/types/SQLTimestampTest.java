package com.splicemachine.db.iapi.types;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
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
public class SQLTimestampTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                SQLTimestamp value = new SQLTimestamp(timestamp);
                SQLTimestamp valueA = new SQLTimestamp();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",timestamp.toString(),valueA.getTimestamp(new GregorianCalendar()).toString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLTimestamp value = new SQLTimestamp();
                SQLTimestamp valueA = new SQLTimestamp();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
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
}
