package com.splicemachine.homeless;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationUtils {

    public static byte[] serializeToBytes(Externalizable e) throws IOException {

        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        byte[] object;

        try{
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(e);
            oos.flush();

            object = bos.toByteArray();
        } finally {
            if(oos != null){
                oos.close();
            }

            if(bos != null){
                bos.close();
            }
        }

        return object;
    }

    public static Object deserializeObject(byte[] b) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try{

            bis = new ByteArrayInputStream(b);
            ois = new ObjectInputStream(bis);

            return ois.readObject();

        } finally {
            if(bis != null){
                bis.close();
            }

            if(ois != null){
                ois.close();
            }
        }

    }


    public static <T extends Externalizable> T roundTripObject(T obj) throws IOException, ClassNotFoundException {
        return (T) deserializeObject( serializeToBytes(obj));
    }

}
