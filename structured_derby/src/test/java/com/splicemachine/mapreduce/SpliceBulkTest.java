package com.splicemachine.mapreduce;

import java.io.IOException;
import java.sql.Types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.splicemachine.derby.impl.load.ColumnContext;
import com.splicemachine.derby.impl.load.ImportContext;
@Ignore
public class SpliceBulkTest {
	private MapDriver<LongWritable, Text, ImmutableBytesWritable, KeyValue> mapDriver;
	private Gson gson = new Gson();
	
	
	@Test
	public void isImportContextGSONSerializable() {		
		Gson gson = new Gson();
		ImportContext importContext = new ImportContext();
		String string1 = gson.toJson(importContext);
		ImportContext importContext2 = gson.fromJson(string1, ImportContext.class);
	}
	
	@Test
	public void testBulkImportMapper() throws IOException, StandardException {
        ColumnContext colCtx = new ColumnContext.Builder()
        .nullable(true)
        .columnType(Types.INTEGER)
        .columnNumber(0).build();
        ImportContext ctx = new ImportContext.Builder()
        .addColumn(colCtx)
        .path("/testPath")
        .destinationTable(1184l)
        .colDelimiter(",")
        .transactionId("123")
        .build();
		mapDriver = new MapDriver(new HBaseBulkLoadMapper());
		mapDriver.withInput(new LongWritable(), new Text("1234"));
		mapDriver.getConfiguration().set(HBaseBulkLoadMapper.IMPORT_CONTEXT, gson.toJson(ctx));
		mapDriver.runTest();
	}
		
}
