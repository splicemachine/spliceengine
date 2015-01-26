package com.splicemachine.mapreduce;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.splicemachine.derby.impl.load.FailAlwaysReporter;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.load.RowParser;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;
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
						byte[] row = kvPair.getRowKey();
						outputKey.set(row);
						outputValue.set(kvPair.getValue());
						context.write(outputKey,outputValue);
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
						entryEncoder = newEntryEncoder(importContext.getTableVersion(), row);
						rowParser = new RowParser(row,importContext, FailAlwaysReporter.INSTANCE);
            if(true)
                throw new UnsupportedOperationException("IMPLEMENT");
//						txnId = Long.parseLong(importContext.getTransactionId());
				} catch (StandardException e) {
						throw new IOException(e);
				}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
				Closeables.closeQuietly(entryEncoder);
				super.cleanup(context);
		}

		private PairEncoder newEntryEncoder(String tableVersion,ExecRow row) {
				int[] pkCols = importContext.getPrimaryKeys();
				KeyEncoder encoder;
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(row);
				if(pkCols!=null&& pkCols.length>0)
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(pkCols, null,serializers), NoOpPostfix.INSTANCE);
				else
						encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null,serializers);
				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
		}

		protected UUIDGenerator getRandomGenerator(){
				/*
				 * You only use MapReduce if you are planning in importing a large number of rows. Thus,
				 * we'll want to buffer up a large number of UUIDs for usage to reduce contention.
				 */
        return com.splicemachine.uuid.Type1UUID.newGenerator(2048);
		}
}