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

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test Class for SQLArray
 *
 * Does not currently support statistics collection...
 *
 */
public class SQLArrayTest extends SQLDataValueDescriptorTest {

        @Test
        public void serdeNullValueData() throws Exception {
                UnsafeRow row = new UnsafeRow(1);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),1);
                SQLArray value = new SQLArray();
                SQLArray valueA = new SQLArray();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", row.isNullAt(0));
                valueA.read(row, 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
        }

        @Test
        public void serdeArrayData() throws Exception {
                UnsafeRow row = new UnsafeRow(2);
                UnsafeRowWriter writer = new UnsafeRowWriter(new BufferHolder(row),2);
                SQLVarchar varchar = new SQLVarchar("sdfsdfdsfsdfsd");
                SQLArray value = new SQLArray(new DataValueDescriptor[]{new SQLInteger(2),new SQLInteger(4)});
                SQLArray valueA = new SQLArray(new DataValueDescriptor[]{new SQLInteger(1)});
                writer.reset();
                varchar.write(writer,0);
                value.write(writer, 1);
                Assert.assertFalse("SerdeIncorrect", row.isNullAt(1));
                valueA.read(row, 1);
                Assert.assertTrue("SerdeIncorrect", value.toString().equals(valueA.toString()));
        }

}
