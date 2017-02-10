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
 * Test Class for SQLBoolean
 *
 */
public class SQLBooleanTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLBoolean value = new SQLBoolean(true);
                SQLBoolean valueA = new SQLBoolean();
                value.write(writer, 0);
                Assert.assertEquals("SerdeIncorrect",true,row.getBoolean(0));
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",true,valueA.getBoolean());
            }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLBoolean value = new SQLBoolean();
                SQLBoolean valueA = new SQLBoolean();
                value.write(writer, 0);
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
        public void rowValueToDVDValue() throws Exception {
                SQLBoolean bool1 = new SQLBoolean(true);
                SQLBoolean bool2 = new SQLBoolean();
                ExecRow execRow = new ValueRow(2);
                execRow.setColumn(1, bool1);
                execRow.setColumn(2, bool2);
                Assert.assertEquals(true, ((Row) execRow).getBoolean(0));
                Assert.assertTrue(((Row) execRow).isNullAt(1));
                Row sparkRow = execRow.getSparkRow();
                GenericRowWithSchema row = new GenericRowWithSchema(new Object[]{true, null}, execRow.schema());
                Assert.assertEquals(row.getBoolean(0), true);
                Assert.assertTrue(row.isNullAt(1));
        }
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
