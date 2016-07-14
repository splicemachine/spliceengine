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
    public static final ExternalizableSerializer INSTANCE = new ExternalizableSerializer();

    private ExternalizableSerializer(){}

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
        } catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException e1) {
            throw new RuntimeException(e1);
        }
    }
}
