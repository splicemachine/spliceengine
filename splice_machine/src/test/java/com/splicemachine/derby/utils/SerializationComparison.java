/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.CountAggregator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;

/**
 * @author Scott Fines
 *         Created on: 8/15/13
 */
@Category(ArchitectureIndependent.class)
public class SerializationComparison {

    @Test
    public void compareJavaAndKryoForAggregators() throws Exception {
        CountAggregator aggregator = new CountAggregator();
        aggregator.accumulate(new SQLInteger(2),null);

        Kryo kryo = new Kryo();
        long start = System.nanoTime();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output,aggregator);
        output.flush();
        byte[] kryoOutput = baos.toByteArray();
        long end = System.nanoTime();

        System.out.printf("Kryo size: %d bytes, ser time: %d ns%n",kryoOutput.length,(end-start));

        start = System.nanoTime();
        ByteDataOutput bdo = new ByteDataOutput();
        bdo.writeObject(aggregator);
        byte[] javaOutput = bdo.toByteArray();
        end = System.nanoTime();
        System.out.printf("Java size: %d bytes, ser time: %d ns%n",javaOutput.length,(end-start));

    }
}
