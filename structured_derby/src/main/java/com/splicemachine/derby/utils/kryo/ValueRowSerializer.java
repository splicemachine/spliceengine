package com.splicemachine.derby.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;

/**
 * @author Scott Fines
 * Created on: 10/10/13
 */
public class ValueRowSerializer extends Serializer<ValueRow> {
    @Override
    public void write(Kryo kryo, Output output, ValueRow object) {
        output.writeInt(object.nColumns());
        DataValueDescriptor[] dvds = object.getRowArray();
        for(DataValueDescriptor dvd:dvds){
            kryo.writeClassAndObject(output,dvd);
        }
    }

    @Override
    public ValueRow read(Kryo kryo, Input input, Class<ValueRow> type) {
        int size = input.readInt();

        DataValueDescriptor[] dvds = new DataValueDescriptor[size];
        for(int i=0;i<dvds.length;i++){
            dvds[i] = (DataValueDescriptor)kryo.readClassAndObject(input);
        }

        ValueRow valueRow = new ValueRow(size);
        valueRow.setRowArray(dvds);
        return valueRow;
    }
}
