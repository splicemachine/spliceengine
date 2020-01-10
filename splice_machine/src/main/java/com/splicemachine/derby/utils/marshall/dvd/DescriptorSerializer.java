/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
