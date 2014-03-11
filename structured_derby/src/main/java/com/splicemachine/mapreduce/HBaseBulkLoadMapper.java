package com.splicemachine.mapreduce;

import au.com.bytecode.opencsv.CSVParser;
import com.google.gson.Gson;
import com.splicemachine.derby.impl.load.FailAlwaysReporter;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.load.RowParser;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Type1UUID;
import com.splicemachine.utils.UUIDGenerator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text,
				ImmutableBytesWritable, ImmutableBytesWritable> {
		public static final String IMPORT_CONTEXT = "import.context";
		private RowParser rowParser;
		private ImportContext importContext;
		private PairEncoder entryEncoder;
		private CSVParser parser;
		private long txnId;

		private ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
		private ImmutableBytesWritable outputValue = new ImmutableBytesWritable();

		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
				String[] parsedRow = parser.parseLine(value.toString());
				try {
						ExecRow execRow = rowParser.process(parsedRow,importContext.getColumnInformation());
						KVPair kvPair = entryEncoder.encode(execRow);
						byte[] row = kvPair.getRow();
						outputKey.set(row);
						outputValue.set(kvPair.getValue());
						context.write(outputKey,outputValue);
//						KeyValue dataKv = new KeyValue(row,
//										SpliceConstants.DEFAULT_FAMILY_BYTES,
//										RowMarshaller.PACKED_COLUMN_KEY,
//										txnId,kvPair.getValue());
//						KeyValue siKv =
//						new KeyValue(row,
//										SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,
//										SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
//										txnId,SIConstants.EMPTY_BYTE_ARRAY);
//						context.write(outputKey,dataKv);
//						context.write(outputKey,siKv);
				} catch (StandardException e) {
						e.printStackTrace();
						throw new IOException(e);
				}catch(Throwable t){
						t.printStackTrace();
						throw new IOException(t);
				}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
				super.setup(context);
				Gson gson = new Gson();
				importContext = gson.fromJson(context.getConfiguration().get(IMPORT_CONTEXT),ImportContext.class);
				if (importContext == null) {
						throw new IOException("Import Context is null");
				}
				parser = new CSVParser(importContext.getColumnDelimiter().charAt(0),importContext.getQuoteChar().charAt(0));
				try {
						ExecRow row = ImportTask.getExecRow(importContext);
						entryEncoder = newEntryEncoder(row);
						rowParser = new RowParser(row,importContext, FailAlwaysReporter.INSTANCE);
						txnId = Long.parseLong(importContext.getTransactionId());
				} catch (StandardException e) {
						throw new IOException(e);
				}
		}

		private PairEncoder newEntryEncoder(ExecRow row) {
				int[] pkCols = importContext.getPrimaryKeys();
				KeyEncoder encoder;
				if(pkCols!=null&& pkCols.length>0)
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(pkCols, null), NoOpPostfix.INSTANCE);
				else
						encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null);
				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
		}

		protected UUIDGenerator getRandomGenerator(){
				/*
				 * You only use MapReduce if you are planning in importing a large number of rows. Thus,
				 * we'll want to buffer up a large number of UUIDs for usage to reduce contention.
				 */
				return Type1UUID.newGenerator(2048);
		}
}