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

package com.splicemachine.derby.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Mark Sirek
 *         Created on: 12/31/18
 */
public abstract class ListDataTypeSerializer<T extends DataValueDescriptor> extends DataValueDescriptorSerializer<T> {
    
    
    // Always read and write a ListDataType object because isNull
    // just means, "at least one of the items in the list is null",
    // not "all items in the list are null".
    @Override
    public void write(Kryo kryo, Output output, T object) {
        try {
            writeValue(kryo, output, object);
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }
    
    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        try {
            T dvd = kryo.newInstance(type);
            readValue(kryo, input, dvd);
            return dvd;
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
