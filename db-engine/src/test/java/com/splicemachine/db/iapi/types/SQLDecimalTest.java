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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.Decimal;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;

/**
 *
 * Test Class for SQLDecimal
 *
 */
public class SQLDecimalTest extends SQLDataValueDescriptorTest {

        @Test
        public void addTwo() throws StandardException {
                SQLDecimal decimal1 = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal decimal2 = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal decimal3 = new SQLDecimal(new BigDecimal("100000000000000000000000000000000000000"));
                Assert.assertEquals("Integer Add Fails", new BigDecimal(200.0d), decimal1.plus(decimal1, decimal2, null).getBigDecimal());
        }

        @Test
        public void subtractTwo() throws StandardException {
                SQLDecimal decimal1 = new SQLDecimal(new BigDecimal(200.0d));
                SQLDecimal decimal2 = new SQLDecimal(new BigDecimal(100.0d));
                Assert.assertEquals("Integer subtract Fails", new BigDecimal(100.0d), decimal1.minus(decimal1, decimal2, null).getBigDecimal());
        }

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row), 1);
                SQLDecimal value = new SQLDecimal(new BigDecimal(100.0d));
                SQLDecimal valueA = new SQLDecimal(null, value.precision, value.getScale());
                writer.reset();
                value.write(writer, 0);
                Decimal sparkDecimal = row.getDecimal(0, value.getDecimalValuePrecision(), value.getDecimalValueScale());
                scala.math.BigDecimal foo = sparkDecimal.toBigDecimal();
                BigDecimal decimal = sparkDecimal.toBigDecimal().bigDecimal();
                Assert.assertEquals("SerdeIncorrect", new BigDecimal("100"), sparkDecimal.toBigDecimal().bigDecimal());
                valueA.read(row, 0);
                Assert.assertEquals("SerdeIncorrect", new BigDecimal("100"), valueA.getBigDecimal());
        }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row), 1);
                SQLDecimal value = new SQLDecimal();
                SQLDecimal valueA = new SQLDecimal();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
        }


        @Test
        public void testColumnStatistics() throws Exception {
                SQLDecimal value1 = new SQLDecimal();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLDecimal SQLDecimal;
                for (int i = 1; i <= 10000; i++) {
                        if (i >= 5000 && i < 6000)
                                SQLDecimal = new SQLDecimal();
                        else if (i >= 1000 && i < 2000)
                                SQLDecimal = new SQLDecimal("" + (1000 + i % 20));
                        else
                                SQLDecimal = new SQLDecimal("" + i);
                        stats.update(SQLDecimal);
                }
                stats = serde(stats);
                Assert.assertEquals(1000, stats.nullCount());
                Assert.assertEquals(9000, stats.notNullCount());
                Assert.assertEquals(10000, stats.totalCount());
                Assert.assertEquals(new SQLDecimal("" + 10000), stats.maxValue());
                Assert.assertEquals(new SQLDecimal("" + 1), stats.minValue());
                Assert.assertEquals(1000, stats.selectivity(null));
                Assert.assertEquals(1000, stats.selectivity(new SQLDecimal()));
                Assert.assertEquals(55, stats.selectivity(new SQLDecimal("" + 1010)));
                Assert.assertEquals(1, stats.selectivity(new SQLDecimal("" + 9000)));
                Assert.assertEquals(1000.0d, (double) stats.rangeSelectivity(new SQLDecimal("" + 1000), new SQLDecimal("" + 2000), true, false), RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(500.0d, (double) stats.rangeSelectivity(new SQLDecimal(), new SQLDecimal("" + 500), true, false), RANGE_SELECTIVITY_ERRROR_BOUNDS);
                Assert.assertEquals(4008.0d, (double) stats.rangeSelectivity(new SQLDecimal("" + 5000), new SQLDecimal(), true, false), RANGE_SELECTIVITY_ERRROR_BOUNDS);
        }

        @Test
        public void testArray() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row), 1);
                SQLArray value = new SQLArray();
                SQLDecimal decimal = new SQLDecimal();
                decimal.setPrecision(10);
                decimal.setScale(2);
                value.setType(decimal);
                value.setValue(new DataValueDescriptor[]{new SQLDecimal(new BigDecimal(23), 10, 2), new SQLDecimal(new BigDecimal(48), 10, 2), new SQLDecimal(new BigDecimal(10), 10, 2), new SQLDecimal()});
                SQLArray valueA = new SQLArray();
                valueA.setType(decimal);
                writer.reset();
                value.write(writer, 0);
                valueA.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", Arrays.equals(value.value, valueA.value));

        }

        @Test
        public void testExecRowSparkRowConversion() throws StandardException {
                ValueRow execRow = new ValueRow(1);

                double initialValue = 10d;
                double val;
                for (int i = 1; i <= 38; i++) {
                        val = java.lang.Math.pow(initialValue, i);
                        execRow.setRowArray(new DataValueDescriptor[]{new SQLDecimal(new BigDecimal(val))});
                        Row row = execRow.getSparkRow();
                        Assert.assertEquals(new BigDecimal(val), row.getDecimal(0));
                        ValueRow execRow2 = new ValueRow(1);
                        execRow2.setRowArray(new DataValueDescriptor[]{new SQLDecimal()});
                        execRow2.getColumn(1).setSparkObject(row.get(0));
                        Assert.assertEquals("ExecRow Mismatch", execRow, execRow2);
                }
        }

        @Test
        public void testSelectivityWithParameter() throws Exception {
                /* let only the first 3 rows take different values, all remaining rows use a default value */
                SQLDecimal value1 = new SQLDecimal();
                ItemStatistics stats = new ColumnStatisticsImpl(value1);
                SQLDecimal sqlDecimal;
                sqlDecimal = new SQLDecimal(new BigDecimal(1.0));
                stats.update(sqlDecimal);
                sqlDecimal = new SQLDecimal(new BigDecimal(2.0));
                stats.update(sqlDecimal);
                sqlDecimal = new SQLDecimal(new BigDecimal(3.0));
                stats.update(sqlDecimal);
                for (int i = 3; i < 81920; i++) {
                        sqlDecimal = new SQLDecimal(new BigDecimal(-1.0));
                        stats.update(sqlDecimal);
                }
                stats = serde(stats);

                /* selectivityExcludingValueIfSkewed() is the function used to compute the electivity of equality
                   predicate with parameterized value
                 */
                double range = stats.selectivityExcludingValueIfSkewed(sqlDecimal);
                Assert.assertTrue(range + " did not match expected value of 1.0d", (range == 1.0d));
        }

        @Test
        public void serdeValueDataExternal() throws Exception {

                double initialValue = 10d;
                double val;
                for (int i = 1; i <= 38; i++) {
                        val = java.lang.Math.pow(initialValue, i);
                        SQLDecimal value = new SQLDecimal(new BigDecimal(val));
                        SQLDecimal valueA = new SQLDecimal(null, value.precision, value.getScale());
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(8192);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        value.writeExternal(objectOutputStream);
                        objectOutputStream.flush();

                        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        valueA.readExternal(objectInputStream);
                        Assert.assertEquals("SerdeIncorrect", new BigDecimal(val), valueA.getBigDecimal());

                        objectInputStream.close();
                        objectOutputStream.close();
                }
        }

        @Test
        public void serdeNullValueDataExternal() throws Exception {
                SQLDecimal value = new SQLDecimal();
                SQLDecimal valueA = new SQLDecimal();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(8192);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                value.writeExternal(objectOutputStream);
                objectOutputStream.flush();
                ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                valueA.readExternal(objectInputStream);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());

                objectInputStream.close();
                objectOutputStream.close();
        }

        @Test
        public void serdeNullValueDataExternalWithPrecisionScale() throws Exception {
                SQLDecimal value = new SQLDecimal(null,5,2);
                SQLDecimal valueA = new SQLDecimal();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(8192);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                value.writeExternal(objectOutputStream);
                objectOutputStream.flush();
                ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                valueA.readExternal(objectInputStream);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
                Assert.assertEquals("Incorrect Precision", -1, valueA.precision);
                Assert.assertEquals("Incorrect Scale", -1, valueA.scale);

                objectInputStream.close();
                objectOutputStream.close();
        }

}