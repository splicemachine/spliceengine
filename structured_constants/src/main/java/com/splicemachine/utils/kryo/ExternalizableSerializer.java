package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Externalizable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 8/15/13
 */
public class ExternalizableSerializer extends Serializer<Externalizable> {
    @Override
    public void write(Kryo kryo, Output output, Externalizable object) {
        KryoObjectOutput koo = new KryoObjectOutput(output, kryo);
        try {
            object.writeExternal(koo);
        } catch (IOException e) {
            //shouldn't happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public Externalizable read(Kryo kryo, Input input, Class<Externalizable> type) {
        try {
            Externalizable e = type.newInstance();
            KryoObjectInput koi = new KryoObjectInput(input,kryo);
            e.readExternal(koi);
            return e;
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        } catch (ClassNotFoundException e1) {
            throw new RuntimeException(e1);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
