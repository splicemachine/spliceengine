/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.utils.test.TestingDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;

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

    @Test
    public void testLongBufferedSumAggregator() throws IOException, StandardException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        LongBufferedSumAggregator lbsa = new LongBufferedSumAggregator(64);
        lbsa.add(new SQLLongint(100));
        lbsa.add(new SQLLongint(200));
        kryo.writeClassAndObject(output, lbsa);
        output.close();
        InputStream in = new ByteArrayInputStream(out.toByteArray());
        Input input = new Input(in);
        LongBufferedSumAggregator lbsa2 = (LongBufferedSumAggregator) kryo.readClassAndObject(input);
        assertEquals(lbsa.getResult(), lbsa2.getResult());
        input.close();
    }

    @Test
    public void testDoubleBufferedSumAggregator() throws IOException, StandardException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        DoubleBufferedSumAggregator lbsa = new DoubleBufferedSumAggregator(64);
        lbsa.add(new SQLDouble(100));
        lbsa.add(new SQLDouble(200));
        kryo.writeClassAndObject(output, lbsa);
        output.close();
        InputStream in = new ByteArrayInputStream(out.toByteArray());
        Input input = new Input(in);
        DoubleBufferedSumAggregator lbsa2 = (DoubleBufferedSumAggregator) kryo.readClassAndObject(input);
        assertEquals(lbsa.getResult(), lbsa2.getResult());
        input.close();
    }

    @Test
    public void testDecimalBufferedSumAggregator() throws IOException, StandardException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        DecimalBufferedSumAggregator lbsa = new DecimalBufferedSumAggregator(64);
        lbsa.add(new SQLDecimal(new BigDecimal(23.0)));
        lbsa.add(new SQLDecimal(new BigDecimal(23.0)));
        kryo.writeClassAndObject(output, lbsa);
        output.close();
        InputStream in = new ByteArrayInputStream(out.toByteArray());
        Input input = new Input(in);
        DecimalBufferedSumAggregator lbsa2 = (DecimalBufferedSumAggregator) kryo.readClassAndObject(input);
        assertEquals(lbsa.getResult(), lbsa2.getResult());
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
