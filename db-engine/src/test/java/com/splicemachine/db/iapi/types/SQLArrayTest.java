/*
 * Copyright 2012 - 2019 Splice Machine, Inc.
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
                UnsafeRowWriter writer = new UnsafeRowWriter(1);
                SQLArray value = new SQLArray();
                SQLArray valueA = new SQLArray();
                value.write(writer, 0);
                Assert.assertTrue("SerdeIncorrect", writer.getRow().isNullAt(0));
                value.read(writer.getRow(), 0);
                Assert.assertTrue("SerdeIncorrect", valueA.isNull());
        }

}
