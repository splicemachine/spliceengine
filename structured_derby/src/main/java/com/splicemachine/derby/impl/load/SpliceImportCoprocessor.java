package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.primitives.Longs;
import com.gotometrics.orderly.*;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

/**
 * Coprocessor for Importing data from a CSV stored in an HDFS BlockLocation.
 *
 * @author Scott Fines
 */
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
		SpliceLogUtils.trace(LOG,"doImport for sourceFile %s,destinationTable %s, with delimiter %s, with columnTypes=%s",
																			sourceFile,destTable,delimiter, Arrays.toString(columnTypes));
		long numImported=0l;
		Path path = new Path(sourceFile);
		Splitter splitter = Splitter.on(delimiter).trimResults();
		FSDataInputStream is = null;
		//get a bulk-insert table for our table to insert
		HTableInterface table = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(destTable));

		LineReader reader = null;
		//open a serializer to serialize our data
		Serializer serializer = new Serializer();
		try{
			is = fs.open(path);
			for(BlockLocation location:locations){
				SpliceLogUtils.trace(LOG,"importing block location %s",location);

				/*
				 * If we aren't the first block location in the file, skip the first line.
				 * Otherwise, we might end up inserting a partial row which wouldn't be good.
				 */
				boolean skipFirstLine = Longs.compare(location.getOffset(),0l)==0;

				//get the start of the location and seek to it
				long start = location.getOffset();
				long end = start + location.getLength();
				is.seek(start);


				reader = new LineReader(is);
				Text text = new Text();
				if(skipFirstLine){
					SpliceLogUtils.trace(LOG,"Skipping first line as other regions will deal with it");
					start = reader.readLine(text);
				}
				long pos = start;
				while(pos < end){
					long newSize = reader.readLine(text);
					SpliceLogUtils.trace(LOG,"inserting line %s",text);
					pos+=newSize;

					Put put = new Put(SpliceUtils.getUniqueKey());
					String line = text.toString();
					int colPos = 0;
					for(String col:splitter.split(line)){
						//go to the next non-null position
						while(!activeCols.get(colPos)){
							colPos++;
							if(colPos>columnTypes.length)
								throw new IOException("Incorrect Column types or index present");
						}
						SpliceLogUtils.trace(LOG,"placing item %s at column position %d",col,colPos);
						put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, 
								Integer.toString(colPos).getBytes(),serializer.serialize(col,columnTypes[colPos]));
						colPos++;
					}
					//do the insert
					table.put(put);
					numImported++;
				}
			}
		}catch(Exception e){
			SpliceLogUtils.logAndThrowRuntime(LOG, "Unexpected error importing block locations", e);
		}finally{
			SpliceLogUtils.trace(LOG,"Finished importing all Block locations, closing table and streams");
			//make sure that all the inserts are flushed out to their respective locations.
			table.flushCommits();
			table.close();
			if(is!=null)is.close();
			if(reader!=null)reader.close();
		}
		SpliceLogUtils.trace(LOG,"Imported %d rows",numImported);
		return numImported;
	}

/*****************************************************************************************************************/
	/*private helper stuff*/

	/*
	 * Convenience object to perform serializations without relying on DataValueDescriptors
	 */
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
