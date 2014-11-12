package com.splicemachine.derby.hbase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLVarchar;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
public class ActivationSerializerTest {

    @Test
    public void testCanCorrectlySerializeDataValueStorageInteger() throws Exception {
        SQLInteger dvd = new SQLInteger(12);
        ActivationSerializer.DataValueStorage storage = new ActivationSerializer.DataValueStorage(dvd);

        Kryo kryo = SpliceDriver.getKryoPool().get();
        Output out = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(out,kryo);
        koo.writeObject(storage);

        byte[] data = out.toBytes();

        Input input = new Input(data);

        KryoObjectInput koi = new KryoObjectInput(input,kryo);
        ActivationSerializer.DataValueStorage dvs = (ActivationSerializer.DataValueStorage)koi.readObject();

        Assert.assertEquals("Incorrect ser/de", dvd.getString(), ((DataValueDescriptor) dvs.getValue(null)).getString());
    }

    @Test
    public void testCanCorrectlySerializeDataValueStorageVarchar() throws Exception {
        SQLVarchar dvd = new SQLVarchar("h");
        ActivationSerializer.DataValueStorage storage = new ActivationSerializer.DataValueStorage(dvd);

        Kryo kryo = SpliceDriver.getKryoPool().get();
        Output out = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(out,kryo);
        koo.writeObject(storage);

        byte[] data = out.toBytes();

        Input input = new Input(data);

        KryoObjectInput koi = new KryoObjectInput(input,kryo);
        ActivationSerializer.DataValueStorage dvs = (ActivationSerializer.DataValueStorage)koi.readObject();

        Assert.assertEquals("Incorrect ser/de", dvd.getString(), ((DataValueDescriptor) dvs.getValue(null)).getString());
    }

    @Test
    public void testCanCorrectlySerializeDataValueStorage() throws Exception {
        SQLChar dvd = new SQLChar("h");
        ActivationSerializer.DataValueStorage storage = new ActivationSerializer.DataValueStorage(dvd);

        Kryo kryo = SpliceDriver.getKryoPool().get();
        Output out = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(out,kryo);
        koo.writeObject(storage);

        byte[] data = out.toBytes();

        Input input = new Input(data);

        KryoObjectInput koi = new KryoObjectInput(input,kryo);
        ActivationSerializer.DataValueStorage dvs = (ActivationSerializer.DataValueStorage)koi.readObject();

        Assert.assertEquals("Incorrect ser/de", dvd.getString(), ((DataValueDescriptor) dvs.getValue(null)).getString());
    }
}
