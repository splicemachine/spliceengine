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

package com.splicemachine.mrio.api.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import com.splicemachine.mrio.api.core.SMSplit;

public class SMHiveSplit extends FileSplit implements InputSplit {
	  protected SMSplit split;

	  public SMHiveSplit() throws IOException{
	    super((Path) null, 0, 0, (String[]) null);
	    split = new SMSplit();
	  }

	  public SMHiveSplit(SMSplit split) {
	    super((Path) null, 0, 0, (String[]) null);
	    this.split = split;
	  }


	  public SMHiveSplit(SMSplit split, Path dummyPath) {
        super(dummyPath, 0, 0, (String[]) null);
	    this.split = split;
	  }

	  public TableSplit getSplit() {
	    return this.split.getSplit();
	  }

	  public SMSplit getSMSplit() {
		    return this.split;
	 }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    split.readFields(in);
	  }

	  @Override
	  public String toString() {
	    return "TableSplit " + split;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    split.write(out);
	  }

	  @Override
	  public long getLength() {
	    return split.getLength();
	  }

	  @Override
	  public String[] getLocations() throws IOException {
		  return split.getLocations();
	  }
	}

