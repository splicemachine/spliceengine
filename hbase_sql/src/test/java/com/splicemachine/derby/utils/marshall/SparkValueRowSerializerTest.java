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

package com.splicemachine.derby.utils.marshall;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class SparkValueRowSerializerTest {

    private static Kryo kryo;

    @BeforeClass
    public static void setup() {
        kryo = new Kryo();
        new SpliceSparkKryoRegistrator().registerClasses(kryo);
    }

    @Test
    public void testEncodingDecodingSeveralRows() throws IOException, StandardException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);

        for (int i = 0; i < 100; i++) {
            kryo.writeClassAndObject(output, getExecRow(i, 13));
        }
        output.close();

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        Input input = new Input(in);
        for (int i = 0; i < 100; i++) {
            ExecRow row = (ExecRow) kryo.readClassAndObject(input);
            assertEquals(i, row.getRowArray()[0].getInt());
            assertEquals(""+i, row.getRowArray()[1].getString());
        }
        input.close();
    }

    public static ExecRow getExecRow(int value, int numberOfRecords) {
        try {
            ValueRow vr = new ValueRow(numberOfRecords);
            for (int i = 0; i<numberOfRecords;i++) {
                DataValueDescriptor dvd;
                switch (i % 3) {
                    case 0:
                        dvd = TestingDataType.INTEGER.getDataValueDescriptor();
                        dvd.setValue(value);
                        vr.setColumn(i + 1, dvd);
                        break;
                    case 1:
                        dvd = TestingDataType.VARCHAR.getDataValueDescriptor();
                        dvd.setValue("" + value);
                        vr.setColumn(i + 1, dvd);
                        break;
                    case 2:
                        dvd = TestingDataType.BIGINT.getDataValueDescriptor();
                        dvd.setValue(value);
                        vr.setColumn(i + 1, dvd);
                        break;
                }
            }
            return vr;
        } catch (StandardException se) {
            return null;
        }
    }
}