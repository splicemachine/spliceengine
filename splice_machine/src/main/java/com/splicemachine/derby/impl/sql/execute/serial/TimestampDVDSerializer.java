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

package com.splicemachine.derby.impl.sql.execute.serial;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import org.joda.time.DateTime;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/12/14
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TimestampDVDSerializer implements DVDSerializer {

		@Override
		public void deserialize(DataValueDescriptor ldvd, byte[] bytes, int offset, int length, boolean desc) throws Exception {
				ldvd.setValue(new DateTime(Encoding.decodeLong(bytes,offset,desc)));
		}

		@Override
    public byte[] serialize(DataValueDescriptor dvd) throws Exception {
        return Encoding.encode(dvd.getDateTime().getMillis());
    }

		@Override
		public byte[] serialize(DataValueDescriptor obj, boolean desc) throws Exception {
				return Encoding.encode(obj.getDateTime().getMillis(),desc);
		}
}
