package com.splicemachine.derby.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.utils.ByteDataOutput;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * @author Scott Fines
 *         Created on: 8/15/13
 */
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
