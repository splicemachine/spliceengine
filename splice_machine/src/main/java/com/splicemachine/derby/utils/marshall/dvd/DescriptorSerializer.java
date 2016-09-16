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

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.Closeable;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public interface DescriptorSerializer extends Closeable {

    void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException;

    byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException;

    void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException;

    void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException;

    boolean isScalarType();

    boolean isFloatType();

    boolean isDoubleType();

    interface Factory<T extends DescriptorSerializer> {

        T newInstance();

        /**
         * @param dvd the dvd to encode/decode
         * @return true if this serializer can be used to encode/decode this DataValueDescriptor
         */
        boolean applies(DataValueDescriptor dvd);

        /**
         * @param typeFormatId the Derby type format id for the descriptor
         * @return true if this serializer can be used to encode/decode DataValueDescriptors with
         * this typeFormatId
         */
        boolean applies(int typeFormatId);

        /**
         * Here "scalar" means the type is ultimately encoded as a variable length integer.
         */
        boolean isScalar();

        boolean isFloat();

        boolean isDouble();
    }
}
