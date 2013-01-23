package com.ir.hbase.index.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;

public class DeleteColumnMapper extends TableMapper<NullWritable, Delete> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		Delete delete = new Delete(key.get());
		Iterator<KeyValue> itr = value.list().iterator();
		while (itr.hasNext()) {
			delete.deleteColumn(itr.next().getFamily(), itr.next().getQualifier(), itr.next().getTimestamp());
		}
		context.write(NullWritable.get(), delete);
	}
}