package com.splicemachine.derby.impl.sql.execute;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.StringDataValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
public class LazyStringDataValueDescriptorTest {

    @Test
    public void testCanSerializeAndDeserializeProperly() throws Exception {
        StringDataValue actual = new SQLChar("h");
        LazyStringDataValueDescriptor dvd = new LazyStringDataValueDescriptor(actual);

        Kryo kryo = SpliceDriver.getKryoPool().get();
        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyStringDataValueDescriptor deserialized = (LazyStringDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception {
        StringDataValue actual = new SQLChar();
        LazyStringDataValueDescriptor dvd = new LazyStringDataValueDescriptor(actual);

        Kryo kryo = SpliceDriver.getKryoPool().get();
        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyStringDataValueDescriptor deserialized = (LazyStringDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());

    }
}
