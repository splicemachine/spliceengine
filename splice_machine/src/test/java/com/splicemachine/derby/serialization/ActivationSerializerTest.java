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

package com.splicemachine.derby.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
@Category(ArchitectureIndependent.class)
public class ActivationSerializerTest {
    private static final KryoPool kp = new KryoPool(1);
    static{
        kp.setKryoRegistry(new SpliceKryoRegistry());
    }

    @Test
    public void testCanCorrectlySerializeDataValueStorageInteger() throws Exception {
        SQLInteger dvd = new SQLInteger(12);
        ActivationSerializer.DataValueStorage storage = new ActivationSerializer.DataValueStorage(dvd);

        Kryo kryo = kp.get();
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

        Kryo kryo = kp.get();
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

        Kryo kryo = kp.get();
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
