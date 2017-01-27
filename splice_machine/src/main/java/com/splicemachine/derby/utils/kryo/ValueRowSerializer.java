/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;

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
