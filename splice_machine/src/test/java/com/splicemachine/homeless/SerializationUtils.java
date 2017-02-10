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
