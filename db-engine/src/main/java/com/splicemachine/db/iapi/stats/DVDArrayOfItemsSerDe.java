/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
