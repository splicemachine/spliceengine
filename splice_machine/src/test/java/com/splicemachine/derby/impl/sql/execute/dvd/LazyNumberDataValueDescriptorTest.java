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

package com.splicemachine.derby.impl.sql.execute.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
@Category(ArchitectureIndependent.class)
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
