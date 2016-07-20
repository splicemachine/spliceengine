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

import java.math.BigDecimal;

/**
 *
 * Test Class for SQLDecimal
 *
 */
public class SQLDecimalTest {

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
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLDecimal value = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal valueA = new SQLDecimal();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertEquals("SerdeIncorrect",new BigDecimal(100.0d),row.getDecimal(0,value.getDecimalValueScale(),value.getDecimalValuePrecision()).toJavaBigDecimal());
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",new BigDecimal(100.0d),valueA.getBigDecimal());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLDecimal value = new SQLDecimal();
                SQLDecimal valueA = new SQLDecimal();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
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
                Assert.assertEquals("1 incorrect",value1.getBigDecimal(),value1a.getBigDecimal());
                Assert.assertEquals("2 incorrect",value2.getBigDecimal(),value2a.getBigDecimal());
        }
}
