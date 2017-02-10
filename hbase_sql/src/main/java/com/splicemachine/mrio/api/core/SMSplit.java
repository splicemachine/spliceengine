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

package com.splicemachine.mrio.api.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.splicemachine.access.HConfiguration;

//import org.apache.hadoop.mapred.SplitLocationInfo;

public class SMSplit extends FileSplit {
	protected TableSplit split;
	  public SMSplit() throws IOException{
		    super(FSUtils.getRootDir(HConfiguration.unwrapDelegate()), 0, 0,null);
		    split = new TableSplit();
		  }
	  
		  public SMSplit(TableSplit split) throws IOException{
		    super(FSUtils.getRootDir(HConfiguration.unwrapDelegate()), 0, 0, null);
		    this.split = split;
		  }

		  public TableSplit getSplit() {
		    return this.split;
		  }

		@Override
		public long getLength() {
			return this.split.getLength();
		}

		@Override
		public String toString() {
			return this.split.toString();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.split.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.split.readFields(in);
		}

		@Override
		public String[] getLocations() throws IOException {
			return this.split.getLocations();
		}

		/*
		@Override
		public SplitLocationInfo[] getLocationInfo() throws IOException {
			return this.split.getLocationInfo();
		}
	*/
		
}
