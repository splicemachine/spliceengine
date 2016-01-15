package com.splicemachine.derby.impl.sql.execute.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
public class LazyNumberDataValueDescriptorTest{
    private static final KryoPool kp=new KryoPool(1);

    static{
        kp.setKryoRegistry(new SpliceKryoRegistry());
    }

    @Test
    public void testCanSerializeAndDeserializeProperly() throws Exception{
        NumberDataValue actual=new SQLDouble(12);
        LazyNumberDataValueDescriptor dvd=new LazyDouble(actual);

        Kryo kryo=kp.get();
        Output output=new Output(4096,-1);
        KryoObjectOutput koo=new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes=output.toBytes();

        Input input=new Input(bytes);
        KryoObjectInput koi=new KryoObjectInput(input,kryo);

        LazyNumberDataValueDescriptor deserialized=(LazyNumberDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getDouble(),deserialized.getDouble(),0.0d);
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception{
        NumberDataValue actual=new SQLDouble();
        LazyNumberDataValueDescriptor dvd=new LazyDouble(actual);

        Kryo kryo=kp.get();
        Output output=new Output(4096,-1);
        KryoObjectOutput koo=new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes=output.toBytes();

        Input input=new Input(bytes);
        KryoObjectInput koi=new KryoObjectInput(input,kryo);

        LazyNumberDataValueDescriptor deserialized=(LazyNumberDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.isNull(),deserialized.isNull());

    }

}
