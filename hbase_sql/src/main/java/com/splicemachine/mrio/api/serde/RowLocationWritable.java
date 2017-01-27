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

package com.splicemachine.mrio.api.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.utils.ByteSlice;

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
