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
 * Test Class for SQLChar
 *
 */
public class SQLVarcharTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLChar value = new SQLChar("Splice Machine");
                SQLChar valueA = new SQLChar();
                writer.reset();
                value.write(writer, 0);
                Assert.assertEquals("SerdeIncorrect","Splice Machine",row.getString(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect","Splice Machine",valueA.getString());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLChar value = new SQLChar();
                SQLChar valueA = new SQLChar();
                writer.reset();
                value.write(writer, 0);
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

        @Test
        public void testColumnStatistics() throws Exception {

                SQLVarchar value1 = new SQLVarchar();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLVarchar sqlVarchar;

                for (int i = 0; i < 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlVarchar = new SQLVarchar();
                        else
                                sqlVarchar = new SQLVarchar(new char[]{(char) ('A' + (i%26))});
                        stats.update(sqlVarchar);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLVarchar(new char[]{'Z'}),stats.maxValue());
                Assert.assertEquals(new SQLVarchar(new char[]{'A'}),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLVarchar()));
                Assert.assertEquals(347,stats.selectivity(new SQLVarchar(new char[]{'A'})));
                Assert.assertEquals(347,stats.selectivity(new SQLVarchar(new char[]{'F'})));
                Assert.assertEquals(1404.0d,(double) stats.rangeSelectivity(new SQLVarchar(new char[]{'C'}),new SQLVarchar(new char[]{'G'}),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(700.0d,(double) stats.rangeSelectivity(new SQLVarchar(),new SQLVarchar(new char[]{'C'}),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(2392.0d,(double) stats.rangeSelectivity(new SQLVarchar(new char[]{'T'}),new SQLVarchar(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }


}
