/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import org.apache.commons.lang3.mutable.MutableDouble;

/**
 * @author Mark Sirek
 *         Created on: 02/18/19
 */
public abstract class SimpleObjectSerializer<T> extends Serializer<T> {

    protected abstract void writeValue(Kryo kryo, Output output, T object) throws RuntimeException;
    protected abstract void readValue(Kryo kryo, Input input, T object) throws RuntimeException;

    @Override
    public void write(Kryo kryo, Output output, T object) {
        writeValue(kryo, output, object);
    }
    
    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        T dvd = kryo.newInstance(type);
        readValue(kryo, input, dvd);
        return dvd;
    }
}
