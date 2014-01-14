package com.splicemachine.derby.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

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
