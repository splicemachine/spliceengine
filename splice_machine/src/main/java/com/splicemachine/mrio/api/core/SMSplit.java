package com.splicemachine.mrio.api.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
//import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.splicemachine.constants.SIConstants;

public class SMSplit extends FileSplit {
	protected TableSplit split;
	  public SMSplit() {
		    super(SIConstants.HBASE_ROOT_DIR, 0, 0, (String[]) null);
		    split = new TableSplit();
		  }
	  
		  public SMSplit(TableSplit split) {
		    super(SIConstants.HBASE_ROOT_DIR, 0, 0, (String[]) null);
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
