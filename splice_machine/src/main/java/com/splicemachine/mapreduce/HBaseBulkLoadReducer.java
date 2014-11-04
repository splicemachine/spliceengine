package com.splicemachine.mapreduce;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 2/28/14
 */
public class HBaseBulkLoadReducer extends Reducer<ImmutableBytesWritable,
				ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

		private boolean throwErrorOnDuplicates;
		private long txnId;
		private Counter rowCounter;

		protected void reduce(ImmutableBytesWritable row, java.lang.Iterable<ImmutableBytesWritable> kvs,
													Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue>.Context context)
						throws java.io.IOException, InterruptedException {
				/*
				 * In a good world, we would have NO duplicates--that is, each row will have 1 and only
				 * 1 associated value with it. However, there are situations (unique constraint violations)
				 * where this may not be true. In those cases, we will use the throwErrorOnDuplicates
				 * to determine whether or not to explode.
				 */
				boolean seen = false;
				try{
						for(ImmutableBytesWritable value:kvs){
								byte[] rowBytes = row.copyBytes();
								if(seen){
										if(throwErrorOnDuplicates)
												throw new IOException("Duplicate entries detected for key "+ Bytes.toStringBinary(rowBytes));
								} else seen = true;

								KeyValue dataKv = new KeyValue(rowBytes, SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES,txnId,value.get());
								context.write(row,dataKv);
								rowCounter.increment(1);
						}
				}catch(Throwable t){
						t.printStackTrace();
						throw new IOException(t);
				}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
				super.setup(context);
				rowCounter = context.getCounter("import","rows");
				throwErrorOnDuplicates = context.getConfiguration().getBoolean("import.throwErrorOnDuplicate",true);
				String txnIdStr=  context.getConfiguration().get("import.txnId");
				if(txnIdStr==null)
						throw new IOException("No Transaction id found, unable to create data!");
				try{
						txnId = Long.parseLong(txnIdStr);
				}catch(NumberFormatException nfe){
						throw new IOException("Unknown format for Transaction Id: "+ txnIdStr);
				}
		}
}
