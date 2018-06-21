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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 * Test Class for SQLTinyint
 *
 */
public class SQLRefTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLRef value = new SQLRef(new SQLRowId("1".getBytes()));
                SQLRef valueA = new SQLRef();
                writer.reset();
                value.write(writer, 0);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",(Object) new SQLRowId("1".getBytes()),(Object) valueA.getObject());
                Assert.assertTrue("SerdeIncorrect", !valueA.isNull());

                Object[] data = new Object[1];
                data[0] = new SQLRowId("1".getBytes()).getBytes();
                Row genericRow = new GenericRow(data);
                valueA = new SQLRef();
                valueA.read(genericRow,0);
                Assert.assertTrue("SerdeIncorrect", !valueA.isNull());
        }

        @Test
        public void serdeHBaseRowLocation() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLRef value = new SQLRef(new HBaseRowLocation("1".getBytes()));
                SQLRef valueA = new SQLRef();
                writer.reset();
                value.write(writer, 0);
                valueA.read(row,0);
                Assert.assertEquals("SerdeIncorrect",(Object) new SQLRowId("1".getBytes()),(Object) valueA.getObject());
                Assert.assertTrue("SerdeIncorrect", !valueA.isNull());

                Object[] data = new Object[1];
                data[0] = new HBaseRowLocation("1".getBytes()).getBytes();
                Row genericRow = new GenericRow(data);
                valueA = new SQLRef();
                valueA.read(genericRow,0);
                Assert.assertTrue("SerdeIncorrect", !valueA.isNull());
         }

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLRef value = new SQLRef();
                SQLRef valueA = new SQLRef();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                value.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
        }

        @Test
        public void testArray() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLArray value = new SQLArray();
                value.setType(new SQLRef(new SQLRowId()));
                value.setValue(new DataValueDescriptor[] {new SQLRef(new SQLRowId("1".getBytes())),new SQLRef(new SQLRowId("435".getBytes())),
                        new SQLRef(new SQLRowId("----".getBytes())), new SQLRef(new SQLRowId())});
                SQLArray valueA = new SQLArray();
                valueA.setType(new SQLRef(new SQLRowId()));
                writer.reset();
                value.write(writer,0);
                valueA.read(row,0);
                Assert.assertTrue("SerdeIncorrect", Arrays.equals(value.value,valueA.value));
        }
}
