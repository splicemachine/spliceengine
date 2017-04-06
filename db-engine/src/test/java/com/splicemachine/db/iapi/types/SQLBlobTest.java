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

import org.apache.hadoop.hbase.util.*;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test Class for SQLBlob
 *
 */
public class SQLBlobTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLBlob value = new SQLBlob("1".getBytes());
                SQLBlob valueA = new SQLBlob();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                valueA.read(row,0);
                Assert.assertTrue("SerdeIncorrect",Bytes.equals("1".getBytes(),valueA.getBytes()));
        }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRowWriter writer = new UnsafeRowWriter();
                writer.initialize(new BufferHolder(),1);
                SQLBlob value = new SQLBlob();
                SQLBlob valueA = new SQLBlob();
                value.write(writer, 0);
                UnsafeRow row = new UnsafeRow();
                row.pointTo(writer.holder().buffer,1,writer.holder().cursor);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
        }
    
        @Test
        public void serdeKeyData() throws Exception {
                SQLBlob value1 = new SQLBlob("1".getBytes());
                SQLBlob value2 = new SQLBlob("2".getBytes());
                SQLBlob value1a = new SQLBlob();
                SQLBlob value2a = new SQLBlob();
                PositionedByteRange range1 = new SimplePositionedMutableByteRange(value1.encodedKeyLength());
                PositionedByteRange range2 = new SimplePositionedMutableByteRange(value2.encodedKeyLength());
                value1.encodeIntoKey(range1, Order.ASCENDING);
                value2.encodeIntoKey(range2, Order.ASCENDING);
                Assert.assertTrue("Positioning is Incorrect", Bytes.compareTo(range1.getBytes(), 0, 9, range2.getBytes(), 0, 9) < 0);
                range1.setPosition(0);
                range2.setPosition(0);
                value1a.decodeFromKey(range1);
                value2a.decodeFromKey(range2);
                Assert.assertTrue("1 incorrect",Bytes.equals(value1.getBytes(),value1a.getBytes()));
                Assert.assertTrue("2 incorrect",Bytes.equals(value2.getBytes(),value2a.getBytes()));
        }
}
