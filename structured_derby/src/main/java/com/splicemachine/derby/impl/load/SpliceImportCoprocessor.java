package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.io.Closeables;
import com.google.common.primitives.Longs;
import com.gotometrics.orderly.*;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StringUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
	public long doImport(Collection<BlockLocation> locations,ImportContext context) throws IOException{
		SpliceLogUtils.trace(LOG,"executing import for context %s",context);
		long numImported=0l;
		Path path =  context.getFilePath();
		FSDataInputStream is = null;
		//get a bulk-insert table for our table to insert
		HTableInterface table = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(context.getTableName()));

		LineReader reader = null;
		//open a serializer to serialize our data
		Serializer serializer = new Serializer(context.getStripString(),context.getTimestampFormat());
		try{
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
			CompressionCodec codec = codecFactory.getCodec(path);
			is = fs.open(path);
			for(BlockLocation location:locations){
				SpliceLogUtils.trace(LOG,"importing block location %s",location);

				/*
				 * If we aren't the first block location in the file, skip the first line.
				 * Otherwise, we might end up inserting a partial row which wouldn't be good.
				 */
				boolean skipFirstLine = Longs.compare(location.getOffset(),0l)!=0;

				//get the start of the location and seek to it
				long start = location.getOffset();
				long end = start + location.getLength();
				is.seek(start);

				InputStream stream  = codec!=null?codec.createInputStream(is): is;
				reader = new LineReader(stream);

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

					importRow(context.getColumnTypes(), context.getActiveCols(),
							context.getColumnDelimiter(), table, serializer, text.toString());
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


	@Override
	public long importFile(ImportContext context) throws IOException{
		Path path =  context.getFilePath();

		HTableInterface table = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(context.getTableName()));
		Serializer serializer = new Serializer(context.getStripString(),context.getTimestampFormat());
		InputStream is;
		BufferedReader reader = null;
		long numImported=0l;
		try{
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
			CompressionCodec codec = codecFactory.getCodec(path);
			is = codec!=null?codec.createInputStream(fs.open(path)):fs.open(path);
			reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while((line = reader.readLine())!=null){

				importRow(context.getColumnTypes(),context.getActiveCols(), context.getColumnDelimiter(),table,serializer,line);
				numImported++;
			}
		}catch (Exception e){
			SpliceLogUtils.logAndThrow(LOG,new IOException(e));
		}finally{
			table.flushCommits();
			table.close();
			Closeables.closeQuietly(reader);
		}
		return numImported;
	}

/*****************************************************************************************************************/
	/*private helper stuff*/

	private void importRow(int[] columnTypes, FormatableBitSet activeCols,
												 String columnDelimiter, HTableInterface table,
												 Serializer serializer, String line ) throws IOException {
		/*
		 * Constructs the put and executes it onto the table.
		 */
		Put put = new Put(SpliceUtils.getUniqueKey());
		int colPos = 0;
		SpliceLogUtils.trace(LOG, "parsing line: %s",line);
		for(String col:parseCsvLine(columnDelimiter, line)){
			SpliceLogUtils.trace(LOG, "parsing position %d with value %s into type %s",colPos, col, columnTypes[colPos]);
			if(colPos >= columnTypes.length||colPos<0){
				//we've exhausted all the known columns, so skip all remaining entries on the line
				break;
			}
			try{
				put.add(HBaseConstants.DEFAULT_FAMILY_BYTES,
					Integer.toString(colPos).getBytes(),serializer.serialize(col,columnTypes[colPos]));
			}catch(Exception e){
				e.printStackTrace();
				String errorMessage=String.format("Unable to parse line %s, " +
						"column %d did not serialize correctly: expected type %s for column string %s",
						line,colPos,getTypeString(columnTypes[colPos]),col);
				SpliceLogUtils.error(LOG,new DoNotRetryIOException(errorMessage));
			}
			colPos = activeCols!=null?activeCols.anySetBit(colPos):colPos+1;
		}
		//do the insert
		table.put(put);
	}

	public static String[] parseCsvLine(String columnDelimiter, String line) throws IOException {
		final CSVReader csvReader = new CSVReader(new StringReader(line), columnDelimiter.charAt(0));
		return csvReader.readNext();
	}

	private String getTypeString(int columnType) {
		switch(columnType){
			case Types.BIT: return "Bit";
			case Types.TINYINT: return "TinyInt";
			case Types.SMALLINT: return "SmallInt";
			case Types.INTEGER: return "Integer";
			case Types.BIGINT: return "BigInt";
			case Types.FLOAT: return "Float";
			case Types.REAL: return "Real";
			case Types.DOUBLE: return "Double";
			case Types.NUMERIC: return "Numeric";
			case Types.DECIMAL: return "Decimal";
			case Types.CHAR: return "Char";
			case Types.VARCHAR: return "Varchar";
			case Types.LONGNVARCHAR: return "LongVarChar";
			case Types.DATE: return "Date";
			case Types.TIME :  return "Time";
			case Types.TIMESTAMP: return "Timestamp";
			case Types.BINARY: return "Binary";
			case Types.VARBINARY: return "VarBinary";
			case Types.LONGVARBINARY: return "LongVarBinary";
			case Types.NULL: return "Null";
			default:
				return "Other";
		}
	}


	/*
	 * Convenience object to perform serializations without relying on DataValueDescriptors
	 */
	private static class Serializer{
		private RowKey varBinRowKey;
		private RowKey longKey;
		private RowKey intKey;
		private RowKey stringKey;
		private RowKey decimalRowKey;

		private final String charDelimiter;
		private final SimpleDateFormat timestampFormat;

		private Serializer(String charDelimiter) {
			this(charDelimiter,null);
		}

		public Serializer(String stripString, String timestampFormat) {
			this.charDelimiter = stripString;
			if(timestampFormat==null)
				timestampFormat ="yyyy-mm-dd hh:mm:ss";
			this.timestampFormat = new SimpleDateFormat(timestampFormat);
		}

		byte[] serialize(String column,int columnType) throws IOException {
			String col = preProcessColumn(column);
			switch(columnType){
				case Types.BOOLEAN:
				//TODO -sf- boolean serializer?
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
					return varBinRowKey.serialize(Bytes.toBytes(col));
				case Types.DATE:
					Date date = getDate(col);
					if(longKey==null)longKey = new LongRowKey();
					return longKey.serialize(date.getTime());
				case Types.TIMESTAMP:
					Timestamp ts = getTimestamp(col);
					if(longKey==null)longKey = new LongRowKey();
					return longKey.serialize(ts.getTime());
				case Types.TIME:
					Time time = getTime(col);
					if(longKey==null)longKey = new LongRowKey();
					return longKey.serialize(time.getTime());
				case Types.BIGINT:
					if(longKey==null)
						longKey = new LongRowKey();
					return longKey.serialize(Long.parseLong(col));
				case Types.DOUBLE:
					if(decimalRowKey==null) decimalRowKey = new BigDecimalRowKey();
					return decimalRowKey.serialize(new BigDecimal(col));
				case Types.INTEGER:
					if(intKey ==null)
						intKey = new IntegerRowKey();
					return intKey.serialize(Integer.parseInt(col.trim()));
				case Types.VARCHAR:
				case Types.LONGNVARCHAR:
				case Types.CLOB:
				case Types.SQLXML:
					if(stringKey == null)
						stringKey = new StringRowKey();
					return stringKey.serialize(col);
				case Types.DECIMAL:
					if(decimalRowKey==null)
						decimalRowKey = new BigDecimalRowKey();
					return decimalRowKey.serialize(new BigDecimal(col));
				default:
					throw new IOException("Attempted to serialize unimplemented " +
											"serializable entity "+col+" with col type number "+columnType);
			}
		}

		private Date getDate(String col) throws IOException {
			if(timestampFormat==null) return Date.valueOf(col);
			try {
				return new Date(timestampFormat.parse(col).getTime());
			} catch (ParseException e) {
				throw new IOException(e);
			}
		}

		private Timestamp getTimestamp(String col) throws IOException{
			try {
				return timestampFormat==null? Timestamp.valueOf(col): new Timestamp(timestampFormat.parse(col).getTime());
			} catch (ParseException e) {
				throw new IOException(e);
			}
		}

		private Time getTime(String col) throws IOException{
			try {
				return timestampFormat==null? Time.valueOf(col): new Time(timestampFormat.parse(col).getTime());
			} catch (ParseException e) {
				throw new IOException(e);
			}
		}

		private String preProcessColumn(String column) {
			return column;
			// Commented Out - John Leach - CSVParser already handles this...
		}
	}
}
