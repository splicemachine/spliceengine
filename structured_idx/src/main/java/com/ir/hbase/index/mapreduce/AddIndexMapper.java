package com.ir.hbase.index.mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import com.ir.constants.SchemaConstants;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.index.IndexUtils;

public class AddIndexMapper extends TableMapper<NullWritable, Put>{	
	private Index index;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		index = Index.toIndex(context.getConfiguration().get(SchemaConstants.SERIALIZED_INDEX));
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		Put put = new Put(index.createIndexKey(key.get(), IndexUtils.convertToValueMap(value)));
		for (Column column : index.getAllColumns()) {
			put.add(column.getFamily().getBytes(),column.getColumnName().getBytes(),value.getValue(column.getFamily().getBytes(), column.getColumnName().getBytes()));
		}
		put.add(SchemaConstants.INDEX_BASE_FAMILY_BYTE, SchemaConstants.INDEX_BASE_ROW_BYTE, key.get());
		context.write(NullWritable.get(), put);
	}
}
