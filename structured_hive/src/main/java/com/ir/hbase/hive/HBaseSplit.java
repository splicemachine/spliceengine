package com.ir.hbase.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class HBaseSplit extends FileSplit implements InputSplit {
	  private final TableSplit split;

	  public HBaseSplit() {
	    super((Path) null, 0, 0, (String[]) null);
	    split = new TableSplit();
	  }

	  public HBaseSplit(TableSplit split, Path dummyPath) {
	    super(dummyPath, 0, 0, (String[]) null);
	    this.split = split;
	  }

	  public TableSplit getSplit() {
	    return this.split;
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    split.readFields(in);
	  }

	  @Override
	  public String toString() {
	    return "TableSplit " + split;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    super.write(out);
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
