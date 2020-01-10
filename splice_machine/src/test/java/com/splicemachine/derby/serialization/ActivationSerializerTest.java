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

package com.splicemachine.derby.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.commons.io.IOExceptionWithCause;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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

    @Test
    public void canSerdeStatementTextLargerThanUTF8Limit() throws Exception {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 65535 + 1; i++) {
                sb.append('A');
            }
            SpliceObserverInstructions.ActivationContext context =
                    new SpliceObserverInstructions.ActivationContext(null,
                            true, true,
                            sb.toString(), false, 100,
                            "foo".getBytes());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            context.writeExternal(oos);
            oos.flush();
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ois = new ObjectInputStream(bais);
            SpliceObserverInstructions.ActivationContext context2 = new SpliceObserverInstructions.ActivationContext();
            context2.readExternal(ois);
            Assert.assertEquals("Serde Incorrect",context.getStatementTxt(),context2.getStatementTxt());
        } finally {
            if (oos!=null)
                oos.close();
            if (ois !=null)
                ois.close();
            }

    }
}
