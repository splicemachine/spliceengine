package com.splicemachine.derby.impl.load;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collection;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.gotometrics.orderly.BigDecimalRowKey;
import com.gotometrics.orderly.DoubleRowKey;
import com.gotometrics.orderly.IntegerRowKey;
import com.gotometrics.orderly.LongRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StringRowKey;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceImportCoprocessor extends BaseEndpointCoprocessor implements SpliceImportProtocol{
	private static final Logger LOG = Logger.getLogger(SpliceImportCoprocessor.class);
	private FileSystem fs;
	
	@Override
	public void start(CoprocessorEnvironment env) {
		SpliceLogUtils.trace(LOG,"Starting SpliceImport coprocessor");
		super.start(env);
		try {
			fs = FileSystem.get(env.getConfiguration());
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to start coprocessor: unable to open FileSystem",e);
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) {
		SpliceLogUtils.trace(LOG,"Stopping SpliceImport coprocessor");
		super.stop(env);
	}

	@Override
	public long doImport(String sourceFile,Collection<BlockLocation> locations,
			String destTable,String delimiter,int[] columnTypes,FormatableBitSet activeCols)
			throws IOException {
		SpliceLogUtils.trace(LOG,"doImport for sourceFile %s,destinationTable %s, with delimiter %s",
																			sourceFile,destTable,delimiter);
		long numImported=0l;
		Path path = new Path(sourceFile);
		Splitter splitter = Splitter.on(delimiter).trimResults();
		FSDataInputStream is = null;
		HTableInterface table = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(destTable));
		BufferedReader reader = null;
		Serializer serializer = new Serializer();
		try{
			is = fs.open(path);
			for(BlockLocation location:locations){
				SpliceLogUtils.trace(LOG,"importing block location %s",location);
				boolean skipFirstLine = location.getOffset() ==0;
				long start = location.getOffset();
				long end = start + location.getLength();
				is.seek(start);
				reader = new BufferedReader(new InputStreamReader(is));
				if(skipFirstLine){
					reader.readLine();
				}
				String line = null;
				while(is.getPos() < end){
					line = reader.readLine();
					int pos=0;
					
					Put put = new Put();
					for(String col:splitter.split(line)){
						//go to the next non-null position
						while(!activeCols.get(pos)){
							pos++;
							if(pos>columnTypes.length)
								throw new IOException("Incorrect Column types or index present");
						}
						
						put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, 
								Integer.toString(pos).getBytes(),serializer.serialize(col,columnTypes[pos]));
					}
					//do the insert
					table.put(put);
					numImported++;
				}
			}
		}finally{
			SpliceLogUtils.trace(LOG,"Finished importing all Block locations, closing table and streams");
			table.flushCommits();
			table.close();
			if(is!=null)is.close();
			if(reader!=null)reader.close();
		}
		SpliceLogUtils.trace(LOG,"Imported %d rows",numImported);
		return numImported;
	}
	
	private static class Serializer{
		private RowKey varBinRowKey;
		private RowKey longKey;
		private RowKey intKey;
		private RowKey stringKey;
		private RowKey decimalRowKey;
		private RowKey doubleKey;
		
		byte[] serialize(String column,int columnType) throws IOException {
			switch(columnType){
				case Types.BOOLEAN:
				case Types.SMALLINT:
				case Types.TINYINT:
				case Types.REF:
				case Types.OTHER:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.BINARY:
				case Types.BIT:
					if(varBinRowKey==null)
						varBinRowKey = new VariableLengthByteArrayRowKey();
					return varBinRowKey.serialize(Bytes.toBytes(column));
				case Types.DATE:
				case Types.BIGINT:
				case Types.TIME:
				case Types.TIMESTAMP:
					if(longKey==null)
						longKey = new LongRowKey();
					return longKey.serialize(Long.parseLong(column));
				case Types.DOUBLE:
					if(doubleKey == null)
						doubleKey = new DoubleRowKey();
					return doubleKey.serialize(Double.parseDouble(column));
				case Types.INTEGER:
					if(intKey ==null)
						intKey = new IntegerRowKey();
					return intKey.serialize(Integer.parseInt(column));
				case Types.VARCHAR:
				case Types.LONGNVARCHAR:
				case Types.CLOB:
				case Types.SQLXML:
					if(stringKey == null)
						stringKey = new StringRowKey();
					return stringKey.serialize(column);
				case Types.DECIMAL:
					if(decimalRowKey==null)
						decimalRowKey = new BigDecimalRowKey();
					return decimalRowKey.serialize(new BigDecimal(column));
				default:
					throw new IOException("Attempted to serialize unimplemented " +
											"serializable entity "+column+" with column type number "+columnType);
			}
		}
	}
}
