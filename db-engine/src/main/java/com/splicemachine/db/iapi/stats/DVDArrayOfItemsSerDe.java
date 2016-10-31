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
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.memory.Memory;
import org.spark_project.guava.io.Closeables;
import java.io.*;


/**
 *
 * Implementation to serialize and deserialize items from the corresponding sketching algorithms.
 *
 */
public class DVDArrayOfItemsSerDe extends ArrayOfItemsSerDe<DataValueDescriptor> {
    DataValueDescriptor dvd;
    public DVDArrayOfItemsSerDe(DataValueDescriptor dvd) {
        this.dvd = dvd;
    }

    /**
     *
     * Returns a byte[] representing the array of Data Value Descriptors
     *
     * @param dataValueDescriptors
     * @return
     */
    @Override
    public byte[] serializeToByteArray(DataValueDescriptor[] dataValueDescriptors) {
        ObjectOutputStream outputStream = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(4096);
            outputStream = new ObjectOutputStream(byteArrayOutputStream);
            for (DataValueDescriptor dvd: dataValueDescriptors) {
                dvd.writeExternal(outputStream);
            }
            outputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (outputStream !=null)
                try {
                    Closeables.close(outputStream, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
        }
    }

    /**
     *
     * Transforms byte[] in memory to an actual array.
     *
     * @param memory
     * @param length
     * @return
     */
    @Override
    public DataValueDescriptor[] deserializeFromMemory(Memory memory, int length) {
        ObjectInputStream inputStream = null;
        DataValueDescriptor[] dataValueDescriptorArray = null;
        try {
            dataValueDescriptorArray = new DataValueDescriptor[length];
            byte[] dvdByteArray = new byte[(int)memory.getCapacity()];
            memory.getByteArray(0l,dvdByteArray,0,(int)memory.getCapacity());
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(dvdByteArray);
            inputStream = new ObjectInputStream(byteArrayInputStream);
            for (int i = 0; i<length; i++) {
                dvd.readExternal(inputStream);
                dataValueDescriptorArray[i] = dvd.cloneValue(true);
            }
            return dataValueDescriptorArray;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
            finally {
            if (inputStream !=null)
                try {
                    Closeables.close(inputStream, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
        }
    }

}
