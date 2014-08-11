package com.splicemachine.mrio.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.splicemachine.mrio.sample.WordCount;

public class SpliceMRUnitTest {
	private MapReduceDriver<ImmutableBytesWritable, ExecRow, Text, IntWritable, ImmutableBytesWritable, ExecRow> mapReduceDriver;
	private MapDriver<ImmutableBytesWritable, ExecRow, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, ImmutableBytesWritable, ExecRow> reduceDriver;
	
	@Before
    public void setUp() {
         WordCount.MyMapper mapper = new WordCount.MyMapper();
         WordCount.MyReducer reducer = new WordCount.MyReducer();
         mapDriver = new MapDriver<ImmutableBytesWritable, ExecRow, Text, IntWritable>();
         mapDriver.setMapper(mapper);
         reduceDriver = new ReduceDriver<Text, IntWritable, ImmutableBytesWritable, ExecRow>();
         reduceDriver.setReducer(reducer);
         mapReduceDriver = new MapReduceDriver<ImmutableBytesWritable, ExecRow, Text, IntWritable, ImmutableBytesWritable, ExecRow>();
         mapReduceDriver.setMapper(mapper);
         mapReduceDriver.setReducer(reducer);
         
	}
	
	@Test
	 public void testMap_EMIT() throws IOException{
	     
		 ImmutableBytesWritable inputKey = new ImmutableBytesWritable(Bytes.toBytes("k"));
		 DataValueDescriptor[] data = new DataValueDescriptor[]{
					new SQLVarchar("Hello")
		 };
		 ExecRow inputValue = new ValueRow(data.length);
		 inputValue.setRowArray(data);
	     mapDriver.withInput(inputKey, inputValue);
	     mapDriver.withOutput(new Text("H"), new IntWritable(1));
	     mapDriver.runTest();         
	 }
	
	 @Test
	  public void testReducer() throws IOException {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    DataValueDescriptor[] data = new DataValueDescriptor[]{
				new SQLInteger(1)
	    };
	    ExecRow outputValue = new ValueRow(data.length);
	    outputValue.setRowArray(data);
	    reduceDriver.withInput(new Text("H"), values);
	    reduceDriver.withOutput(new ImmutableBytesWritable(Bytes.toBytes("H")), outputValue);
	    reduceDriver.runTest();
	  }
	 
	 @Test
	  public void testMapReduce() throws IOException {
		 ImmutableBytesWritable inputKey = new ImmutableBytesWritable(Bytes.toBytes("k"));
		 DataValueDescriptor[] data = new DataValueDescriptor[]{
					new SQLVarchar("Hello")
		 };
		 ExecRow inputValue = new ValueRow(data.length);
		 inputValue.setRowArray(data);
		 
		 List<IntWritable> values = new ArrayList<IntWritable>();
		    values.add(new IntWritable(1));
		    DataValueDescriptor[] data2 = new DataValueDescriptor[]{
					new SQLInteger(1)
		    };
		    ExecRow outputValue = new ValueRow(data2.length);
		    outputValue.setRowArray(data2);
	    mapReduceDriver.withInput(inputKey, inputValue);
	    mapReduceDriver.addOutput(new ImmutableBytesWritable(Bytes.toBytes("H")), outputValue);
	    
	    mapReduceDriver.runTest();
	  }
}
