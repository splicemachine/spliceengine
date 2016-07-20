package com.splicemachine.db.iapi.types;

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
 * Test Class for SQLChar
 *
 */
public class SQLCharTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLChar value = new SQLChar("Splice Machine");
                SQLChar valueA = new SQLChar();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertEquals("SerdeIncorrect","Splice Machine",row.getString(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect","Splice Machine",valueA.getString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLChar value = new SQLChar();
                SQLChar valueA = new SQLChar();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
            }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLChar value1 = new SQLChar("Splice Machine");
                SQLChar value2 = new SQLChar("Xplice Machine");
                SQLChar value1a = new SQLChar();
                SQLChar value2a = new SQLChar();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertEquals("1 incorrect",value1.getString(),value1a.getString());
                Assert.assertEquals("2 incorrect",value2.getString(),value2a.getString());
        }
}
