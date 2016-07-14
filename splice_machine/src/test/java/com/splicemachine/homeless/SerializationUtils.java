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
