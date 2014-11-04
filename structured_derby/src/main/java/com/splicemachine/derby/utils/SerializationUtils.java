package com.splicemachine.derby.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class SerializationUtils {

    private SerializationUtils(){}

    public static void writeNullableString(String value, DataOutput out) throws IOException {
        if (value != null) {
            out.writeBoolean(true);
            out.writeUTF(value);
        } else {
            out.writeBoolean(false);
        }
    }

    public static String readNullableString(ObjectInput in) throws IOException{
        if(in.readBoolean())
            return in.readUTF();
        return null;
    }
}
