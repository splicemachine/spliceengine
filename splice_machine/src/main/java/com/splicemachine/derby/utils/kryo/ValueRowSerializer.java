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

package com.splicemachine.derby.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import javax.management.relation.RoleUnresolved;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Scott Fines
 * Created on: 10/10/13
 */
public abstract class ValueRowSerializer<T extends ValueRow> extends Serializer<T> {
    @Override
    public void write(Kryo kryo, Output output, T object) {
        output.writeInt(object.nColumns());
        DataValueDescriptor[] dvds = object.getRowArray();
        for(DataValueDescriptor dvd:dvds){
            kryo.writeClassAndObject(output,dvd);
        }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        int size = input.readInt();

				T instance = newType(size);

        DataValueDescriptor[] dvds = new DataValueDescriptor[size];
        for(int i=0;i<dvds.length;i++){
            dvds[i] = (DataValueDescriptor)kryo.readClassAndObject(input);
        }
				instance.setRowArray(dvds);

				return instance;
    }

		protected abstract T newType(int size);
}
