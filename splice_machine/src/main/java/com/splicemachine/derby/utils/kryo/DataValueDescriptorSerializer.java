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
