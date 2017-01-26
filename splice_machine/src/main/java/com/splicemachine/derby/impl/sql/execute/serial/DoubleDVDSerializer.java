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

package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.db.iapi.types.DataValueDescriptor;


public class DoubleDVDSerializer implements DVDSerializer {


		@Override
    public byte[] serialize(DataValueDescriptor obj) throws Exception {
        return Encoding.encode(obj.getDouble());
    }

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode(obj.getDouble(),desc);
		}

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setValue(Encoding.decodeDouble(bytes,offset,desc));
		}
}
