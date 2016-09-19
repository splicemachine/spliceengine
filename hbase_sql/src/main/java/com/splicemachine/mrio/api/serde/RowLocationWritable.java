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

package com.splicemachine.mrio.api.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class RowLocationWritable implements Writable, WritableComparable{
	HBaseRowLocation rowLocation;
	
	public RowLocationWritable() {

	}
	
	public RowLocation get() {
		return rowLocation;
	}

	public void set(RowLocation rowLocation) {
		this.rowLocation = (HBaseRowLocation) rowLocation;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (rowLocation == null) {
			out.writeInt(0);
			return;
		}
	    int length = rowLocation.getLength();
	    out.writeInt(length);
	    out.write(rowLocation.getSlice().array(), rowLocation.getSlice().offset(), length);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		if (length == 0) {
			rowLocation = null;
			return;
		}
		byte[] buffer = new byte[length];
		in.readFully(buffer, 0, length);
		rowLocation = new HBaseRowLocation(ByteSlice.wrap(buffer));
	}

	@Override
	public int compareTo(Object o) {
		if (rowLocation==null || o==null)
			return -1;
		return this.rowLocation.getSlice().compareTo( ((HBaseRowLocation) o).getSlice());
	}
}
