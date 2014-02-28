package com.splicemachine.mapreduce;

import java.io.IOException;
import java.io.Reader;

import com.splicemachine.hbase.KVPair;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.gson.Gson;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.load.RowParser;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import au.com.bytecode.opencsv.CSVReader;

public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	public static final String IMPORT_CONTEXT = "import.context";
	private String currentValue;
	private CSVReader csvReader;
	private Reader reader;
	private RowParser rowParser;
	private ImportContext importContext;
	private PairEncoder entryEncoder;
	private ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		currentValue = value.toString();
		String[] parsedRow = csvReader.readNext();
		try {
			System.out.println("rowParser " + rowParser);
			System.out.println("importContext " + importContext);
			System.out.println("parsedRow " + parsedRow);

			ExecRow execRow = rowParser.process(parsedRow,importContext.getColumnInformation());
			KVPair kvPair = entryEncoder.encode(execRow);
			outputKey.set(kvPair.getRow());
    		Put put = SpliceUtils.createPut(kvPair.getRow(),importContext.getTransactionId());
    		put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY,Long.parseLong(importContext.getTransactionId()),kvPair.getValue());
    		for (byte[] family:put.getFamilyMap().keySet()) {
    			for (KeyValue keyValue: put.getFamilyMap().get(family)) {
    				context.write(outputKey, keyValue);
    			}
    		}
		} catch (StandardException e) {
			throw new IOException(e);
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
		reader = new Reader() {
			@Override
			public void close() throws IOException {
				// No Op
			}

			@Override
			public int read(char[] cbuf, int off, int len) throws IOException {
				if (currentValue == null)
					return -1;
				int length = currentValue.length();
				if (currentValue.length() < len) {
					currentValue.getChars(off, length, cbuf, 0);
					currentValue = null;
					return length;
				}
				else
					return -1;
			}
			
		};
		csvReader = new CSVReader(reader,importContext.getColumnDelimiter().charAt(0),importContext.getQuoteChar().charAt(0));
		try {
			ExecRow row = ImportTask.getExecRow(importContext);
			entryEncoder = newEntryEncoder(row);
			rowParser = new RowParser(row,importContext);
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
	protected Snowflake.Generator getRandomGenerator(){
		return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
}
}