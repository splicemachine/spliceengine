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
 *
 */

package com.splicemachine.derby.impl.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KryoSerialization {

    private KryoPool kpool = SpliceSparkKryoRegistrator.getInstance();
    private Kryo kryo = null;
    private Input input = new Input(4096);
    private Output output = new Output(4096);

    public void init() {
        kryo = kpool.get();
    }

    public void close() {
        kpool.returnInstance(kryo);
    }

    public byte[] serialize(Object unserialized) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        output.setOutputStream(out);
        kryo.writeClassAndObject(output, unserialized);
        output.flush();
        return out.toByteArray();
    }

    public Object deserialize(byte[] serialized) {
        input.setInputStream( new ByteArrayInputStream(serialized) );
        return kryo.readClassAndObject(input);
    }
}
