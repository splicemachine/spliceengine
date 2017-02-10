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

