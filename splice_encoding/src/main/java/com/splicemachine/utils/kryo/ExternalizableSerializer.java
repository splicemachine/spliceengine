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
            Externalizable e = kryo.newInstance(type);
            KryoObjectInput koi = new KryoObjectInput(input,kryo);
            e.readExternal(koi);
            return e;
        } catch (IOException | ClassNotFoundException e1) {
            throw new RuntimeException(e1);
        }
    }
}
