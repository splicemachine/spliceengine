package com.splicemachine.derby.impl.sql.execute;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.serial.DoubleDVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.apache.derby.iapi.types.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
public class LazyNumberDataValueDescriptorTest {
        @Test
        public void testCanSerializeAndDeserializeProperly() throws Exception {
            NumberDataValue actual = new SQLDouble(12);
            LazyNumberDataValueDescriptor dvd = new LazyNumberDataValueDescriptor(actual,new DoubleDVDSerializer());

            Kryo kryo = SpliceDriver.getKryoPool().get();
            Output output = new Output(4096,-1);
            KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
            koo.writeObject(dvd);

            byte[] bytes = output.toBytes();

            Input input = new Input(bytes);
            KryoObjectInput koi = new KryoObjectInput(input,kryo);

            LazyNumberDataValueDescriptor deserialized = (LazyNumberDataValueDescriptor)koi.readObject();

            Assert.assertEquals("Incorrect serialization/deserialization!", dvd.getDouble(), deserialized.getDouble(),0.0d);
        }

        @Test
        public void testCanSerializeNullsCorrectly() throws Exception {
            NumberDataValue actual = new SQLDouble();
            LazyNumberDataValueDescriptor dvd = new LazyNumberDataValueDescriptor(actual,new DoubleDVDSerializer());

            Kryo kryo = SpliceDriver.getKryoPool().get();
            Output output = new Output(4096,-1);
            KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
            koo.writeObject(dvd);

            byte[] bytes = output.toBytes();

            Input input = new Input(bytes);
            KryoObjectInput koi = new KryoObjectInput(input,kryo);

            LazyNumberDataValueDescriptor deserialized = (LazyNumberDataValueDescriptor)koi.readObject();

            Assert.assertEquals("Incorrect serialization/deserialization!", dvd.isNull(), deserialized.isNull());

        }

}
