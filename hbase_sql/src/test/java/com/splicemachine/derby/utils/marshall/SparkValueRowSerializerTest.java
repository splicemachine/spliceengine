/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.UserType;
import com.splicemachine.db.impl.services.reflect.ReflectClassesJava2;
import com.splicemachine.db.impl.sql.execute.UserDefinedAggregator;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class SparkValueRowSerializerTest {
    private static ClassFactory cf;
    private static UserDefinedAggregator userDefinedAggregator;

    static {
        cf = new ReflectClassesJava2();
        userDefinedAggregator = new UserDefinedAggregator();
        userDefinedAggregator.setup(cf,SpliceStddevSamp.class.getCanonicalName(), DataTypeDescriptor.DOUBLE);

    }

    private static Kryo kryo;

    @BeforeClass
    public static void setup() {
        kryo = new Kryo();
        new SpliceSparkKryoRegistrator().registerClasses(kryo);
    }

    @Test
    public void testSTDDEV_SAMP() throws StandardException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);

        for (int i = 0; i < 10; i++) {
            kryo.writeClassAndObject(output, getCustomAggregateFunctions(13));
        }
        output.close();

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        Input input = new Input(in);
        for (int i = 0; i < 10; i++) {
            ExecRow row = (ExecRow) kryo.readClassAndObject(input);
            Assert.assertTrue(row.getRowArray()[0] instanceof UserType);
            Assert.assertTrue(row.getRowArray()[1].getObject() instanceof UserDefinedAggregator);
        }
        input.close();


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

    public static ExecRow getCustomAggregateFunctions(int numberOfRecords) throws StandardException {
        ValueRow vr = new ValueRow(numberOfRecords);
        for (int i = 0; i<numberOfRecords;i++) {
            ExecAggregator foo = userDefinedAggregator.newAggregator();
            foo.accumulate(new SQLDouble(2.0),null);
            vr.setColumn(i+1,new UserType(foo));
        }
        return vr;
    }



}