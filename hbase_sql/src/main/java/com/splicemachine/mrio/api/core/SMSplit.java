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
