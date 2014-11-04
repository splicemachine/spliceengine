package com.splicemachine.intg.hive;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LazySpliceRow extends LazyStruct{

	private ExecRowWritable row;
	
	public LazySpliceRow(LazySimpleStructObjectInspector oi) {
		super(oi);
		// TODO Auto-generated constructor stub
	}
	
	/**
	   * Set the Splice row data(a Result writable) for this LazyStruct.
	   * @see LazySpliceRow#init(Result)
	   */
	public void init(ExecRowWritable r) {
	    row = r;
	    setParsed(false);
	}
	
	 /**
	   * Parse the Result and fill each field.
	   * @see LazyStruct#parse()
	   */
	  private void parse() {

	   
	    setParsed(true);
	  }
	  
}
