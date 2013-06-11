package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import com.google.common.io.Closeables;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.RowSerializer;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;

/**
 * Coprocessor for Importing data from a CSV stored in an HDFS BlockLocation.
 *
 * @author Scott Fines
 */
@Deprecated
public class SpliceImportCoprocessor extends BaseEndpointCoprocessor {
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

//	@Override
	public long doImport(Collection<BlockLocation> locations,ImportContext context) throws IOException{
		SpliceLogUtils.trace(LOG,"executing import for context %s",context);
		long numImported=0l;
		Path path =  context.getFilePath();
		FSDataInputStream is = null;
		//get a bulk-insert table for our table to insert

		LineReader reader = null;
		//open a serializer to serialize our data
        Serializer serializer = Serializer.get();
        CSVParser csvParser = getCsvParser(context);
        try{
            CallBuffer<Mutation> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(context.getTableName().getBytes());
            ExecRow row = getExecRow(context);
            FormatableBitSet pkCols = context.getPrimaryKeys();
            RowSerializer rowSerializer = new RowSerializer(row.getRowArray(),pkCols,pkCols==null);

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
                    String[] cols = csvParser.parseLine(text.toString());
                    doImportRow(context.getTransactionId(), cols, context.getActiveCols(), row, writeBuffer,rowSerializer,serializer);
					numImported++;
				}
			}
            writeBuffer.flushBuffer();
            writeBuffer.close();
		}catch(Exception e){
			SpliceLogUtils.logAndThrowRuntime(LOG, "Unexpected error importing block locations", e);
		}finally{
			SpliceLogUtils.trace(LOG, "Finished importing all Block locations, closing table and streams");
			if(is!=null)is.close();
			if(reader!=null)reader.close();
		}
		SpliceLogUtils.trace(LOG,"Imported %d rows",numImported);
		return numImported;
	}

//    @Override
	public long importFile(ImportContext context) throws IOException{
		Path path =  context.getFilePath();

        ExecRow row = getExecRow(context);
		InputStream is;
		Reader reader = null;
		long numImported=0l;
        Serializer serializer =  Serializer.get();
        FormatableBitSet pkCols = context.getPrimaryKeys();
		try{
            CallBuffer<Mutation> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(context.getTableName().getBytes());
            RowSerializer rowSerializer = new RowSerializer(row.getRowArray(),pkCols,pkCols==null);
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
			CompressionCodec codec = codecFactory.getCodec(path);
			is = codec!=null?codec.createInputStream(fs.open(path)):fs.open(path);
            reader = new InputStreamReader(is);
            CSVReader csvReader = getCsvReader(reader,context);
            String[] line;
			while((line = csvReader.readNext())!=null){
                doImportRow(context.getTransactionId(), line,context.getActiveCols(), row,writeBuffer,rowSerializer,serializer);

				numImported++;
                if(numImported%100==0){
                    SpliceLogUtils.trace(LOG,"imported %d records",numImported);
                }
			}
            writeBuffer.flushBuffer();
            writeBuffer.close();
		}catch (Exception e){
			SpliceLogUtils.logAndThrow(LOG,new IOException(e));
		}finally{
			Closeables.closeQuietly(reader);
		}
		return numImported;
	}

    /*****************************************************************************************************************/
	/*private helper stuff*/

    private CSVReader getCsvReader(Reader reader, ImportContext context) {
        return new CSVReader(reader,getColumnDelimiter(context),getQuoteChar(context));
    }

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(getColumnDelimiter(context),getQuoteChar(context));
    }

    private char getQuoteChar(ImportContext context) {
        String stripStr = context.getStripString();
        if(stripStr==null||stripStr.length()<=0)
            stripStr = "\"";
        return stripStr.charAt(0);
    }

    private char getColumnDelimiter(ImportContext context) {
        String delimiter = context.getColumnDelimiter();
        if(delimiter==null||delimiter.length()<=0)
            delimiter = ",";
        return delimiter.charAt(0);
    }

    private void doImportRow(String transactionId, String[] line,FormatableBitSet activeCols, ExecRow row,
                             CallBuffer<Mutation> writeBuffer,
                             RowSerializer rowSerializer,Serializer serializer) throws IOException {
        try{
            if(activeCols!=null&&activeCols.getNumBitsSet()>0){
                for(int pos=0,activePos=activeCols.anySetBit();pos<line.length;pos++,activePos=activeCols.anySetBit(activePos)){
                    row.getColumn(activePos+1).setValue(line[pos]);
                }
            }else{
                for(int pos=0;pos<line.length-1;pos++){
                    row.getColumn(pos+1).setValue(line[pos]);
                }
                //the last entry in the line array can be an empty string, which correlates to the row's nColumns() = line.length-1
                if(row.nColumns()==line.length){
                    row.getColumn(line.length).setValue(line[line.length-1]);
                }
            }
            Put put = Puts.buildInsertWithSerializer(rowSerializer.serialize(row.getRowArray()), row.getRowArray(),
                    null, transactionId, serializer);
            writeBuffer.add(put);
        }catch(StandardException se){
            throw new DoNotRetryIOException(se.getMessageId());
        } catch (Exception e) {
            SpliceLogUtils.error(LOG,"Error importing line %s", Arrays.toString(line));
            throw Exceptions.getIOException(e);
        }
    }

    private ExecRow getExecRow(ImportContext context) throws DoNotRetryIOException {
        int[] columnTypes = context.getColumnTypes();
        FormatableBitSet activeCols = context.getActiveCols();
        ExecRow row = new ValueRow(columnTypes.length);
        if(activeCols!=null && activeCols.getNumBitsSet()>0){
            for(int i=activeCols.anySetBit();i!=-1;i=activeCols.anySetBit(i)){
                row.setColumn(i+1,getDataValueDescriptor(columnTypes[i]));
            }
        }else{
            for(int i=0;i<columnTypes.length;i++){
                row.setColumn(i+1,getDataValueDescriptor(columnTypes[i]));
            }
        }
        return row;
    }

    private DataValueDescriptor getDataValueDescriptor(int columnType) throws DoNotRetryIOException {
        DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnType);
        try {
            return td.getNull();
        } catch (StandardException e) {
            throw new DoNotRetryIOException(e.getMessageId());
        }
    }


}
