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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
public abstract class DataValueDescriptorSerializer<T extends DataValueDescriptor> extends Serializer<T> {

    @Override
    public final void write(Kryo kryo, Output output, T object) {
        output.writeBoolean(object.isNull());
        if(!object.isNull()){
            try{
                writeValue(kryo, output, object);
            }catch(StandardException se){
                throw new RuntimeException(se);
            }
        }
    }

    protected abstract void writeValue(Kryo kryo, Output output, T object) throws StandardException;

    @Override
    public final T read(Kryo kryo, Input input, Class<T> type) {
        try {
            T dvd = type.newInstance();
            if(!input.readBoolean())
                readValue(kryo,input,dvd);
            return dvd;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void readValue(Kryo kryo, Input input, T dvd) throws StandardException;
}
