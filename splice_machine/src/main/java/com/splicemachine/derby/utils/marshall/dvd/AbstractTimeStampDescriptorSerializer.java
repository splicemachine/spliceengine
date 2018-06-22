/*
 * Copyright (c) 2018 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * A non-scalar version of AbstractTimeDescriptorSerializer, for serialization
 * of version 3.0 and above Timestamp data type.
 * @author Mark Sirek
 * Date: 6/9/18
 */
public abstract class AbstractTimeStampDescriptorSerializer extends AbstractTimeDescriptorSerializer {

    @Override
    public boolean isScalarType() { return false; }


    public static abstract class Factory implements DescriptorSerializer.Factory {
        @Override
        public boolean applies(DataValueDescriptor dvd) {
            return dvd != null && applies(dvd.getTypeFormatId());
        }

        @Override
        public boolean isScalar() { return false; }

        @Override
        public boolean isFloat() { return false; }

        @Override
        public boolean isDouble() { return false; }
    }
}

