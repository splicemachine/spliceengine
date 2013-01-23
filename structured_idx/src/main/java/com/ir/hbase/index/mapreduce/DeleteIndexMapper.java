package com.ir.hbase.index.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;

public class DeleteIndexMapper extends TableMapper<NullWritable, Delete>{
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Delete(key.get()));
	}
}
