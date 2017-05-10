/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *
 * Test Class for SQLChar
 *
 */
public class SQLCharTest extends SQLDataValueDescriptorTest {

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
        public void rowValueToDVDValue() throws Exception {
                SQLChar char1 = new SQLChar("foobar");
                SQLChar char2 = new SQLChar();
                ExecRow execRow = new ValueRow(2);
                execRow.setColumn(1, char1);
                execRow.setColumn(2, char2);
                Assert.assertEquals("foobar", ((Row) execRow).getString(0));
                Assert.assertTrue(((Row) execRow).isNullAt(1));
                Row sparkRow = execRow.getSparkRow();
                GenericRowWithSchema row = new GenericRowWithSchema(new Object[]{"foobar", null}, execRow.schema());
                Assert.assertEquals(row.getString(0), "foobar");
                Assert.assertTrue(row.isNullAt(1));
        }

        @Test
        public void testSerde() throws Exception {
                SQLChar charString = new SQLChar("foo");
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                charString.writeExternal(objectOutputStream);
                objectOutputStream.flush();

                SQLChar charString2 = new SQLChar();
                ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                charString2.readExternal(objectInputStream);
                System.out.println("charString2 " + charString2);


                //
                //




        }

        @Test
        public void testColumnStatistics() throws Exception {
                SQLChar value1 = new SQLChar();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLChar sqlChar;
                for (int i = 0; i < 10000; i++) {
                        if (i>=5000 && i < 6000)
                                sqlChar = new SQLChar();
                        else
                                sqlChar = new SQLChar(new char[]{(char) ('A' + (i%26))});
                        stats.update(sqlChar);
                }
                stats = serde(stats);
                Assert.assertEquals(1000,stats.nullCount());
                Assert.assertEquals(9000,stats.notNullCount());
                Assert.assertEquals(10000,stats.totalCount());
                Assert.assertEquals(new SQLChar(new char[]{'Z'}),stats.maxValue());
                Assert.assertEquals(new SQLChar(new char[]{'A'}),stats.minValue());
                Assert.assertEquals(1000,stats.selectivity(null));
                Assert.assertEquals(1000,stats.selectivity(new SQLChar()));
                Assert.assertEquals(347,stats.selectivity(new SQLChar(new char[]{'A'})));
                Assert.assertEquals(347,stats.selectivity(new SQLChar(new char[]{'F'})));
                Assert.assertEquals(1404.0d,(double) stats.rangeSelectivity(new SQLChar(new char[]{'C'}),new SQLChar(new char[]{'G'}),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(700.0d,(double) stats.rangeSelectivity(new SQLChar(),new SQLChar(new char[]{'C'}),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(2392.0d,(double) stats.rangeSelectivity(new SQLChar(new char[]{'T'}),new SQLChar(),true,false),RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }


}
