package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.metrics.BaseIOStats;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class FileImportReader implements ImportReader{
    private CsvListReader csvReader;
    private InputStream stream;

		private Timer timer;
		private MeasuredReader reader;
		private Path path;

		private static final int lineBatchSize = 64;
		private String[][] lines;
		private int currentPosition;
		private int mask = (lineBatchSize-1);
		private boolean finished = false;

		@Override
    public void setup(FileSystem fileSystem, ImportContext ctx) throws IOException {
        path = ctx.getFilePath();
        CompressionCodecFactory cdF = new CompressionCodecFactory(SpliceUtils.config);
        CompressionCodec codec = cdF.getCodec(path);
        stream = codec!=null? codec.createInputStream(fileSystem.open(path)):fileSystem.open(path);
        reader = new MeasuredReader(new InputStreamReader(stream));
		timer = ctx.shouldRecordStats() ? Metrics.samplingTimer(SpliceConstants.sampleTimingSize,SpliceConstants.sampleTimingSize) : Metrics.noOpTimer();
        csvReader = getCsvReader(reader,ctx);
		this.lines = new String[lineBatchSize][];
	}

    @Override
    public String[] nextRow() throws IOException {
				if(currentPosition==0){
						if(!fillBatch()) return null;
				}
        String[] next = lines[currentPosition];
				if(next!=null){
						currentPosition = (currentPosition+1) & mask;
				}
				return next;
    }

		protected boolean fillBatch() throws IOException {
				String[] next;
				timer.startTiming();
				int count=0;
				boolean shouldContinue;
				BATCH: for(int i=0;i<lines.length;i++){
						do{
							List<String> nextList = csvReader.read();
								if(nextList==null) {
										Arrays.fill(lines, i, lines.length, null);
										break BATCH;
								}

								/*
								 * This is unfortunate that we need to convert from a List to an array.
								 * However, the object creation and tokenization is better with SuperCSV
								 * which is showing a small 5-10% improvement in import tests over OpenCSV.
								 * Changing from an array to a list has a cascading effect on the code
								 * as you would imagine causing a number of interfaces to change.
								 * Leaving as is for now and will monitor the performance.
								 */
								next = new String[nextList.size()];
								nextList.toArray(next);

								shouldContinue = next.length==0||(next.length==1&&(next[0]==null||next[0].length()==0));
						}while(shouldContinue);
						//can assume that we are non-null here, else we would have broken from our batch
						lines[i] = next;
						count++;
				}
				if(count==0)
						timer.stopTiming();
				else
						timer.tick(count);
				return count>0;
		}

		@Override
		public String[][] nextRowBatch() throws IOException {
				if(!fillBatch()) return null;
				return lines;
		}

		@Override
		public IOStats getStats() {
				//if we didn't read any rows, then we probably didn't record any metrics
				if(timer.getNumEvents()==0) return Metrics.noOpIOStats();

				return new BaseIOStats(timer.getTime(),reader.getCharsRead(),timer.getNumEvents());
		}

		@Override
		public boolean shouldParallelize(FileSystem fs, ImportContext ctx) throws IOException {
				Path filePath = ctx.getFilePath();
				FileStatus fileStatus = fs.getFileStatus(filePath);
				return fileStatus.getLen() > SpliceConstants.sequentialImportThreashold;
		}

		@Override
    public void close() throws IOException {
        Closeables.closeQuietly(stream);
        Closeables.closeQuietly(csvReader);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //nothing to write
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //nothing to write
    }

    private CsvListReader getCsvReader(Reader reader, ImportContext importContext) {
    	/*
    	 * TODO: [DB-2425] Uncomment the last argument to the builder after the Splice version of super-csv is being built or
    	 * the super-csv main github repo has pulled in our changes.
    	 */
    	return new CsvListReader(
    			reader,
    			new CsvPreference.Builder(
    					importContext.getQuoteChar().charAt(0),
    					importContext.getColumnDelimiter().charAt(0),
    					"\n"/*,
    					SpliceConstants.importMaxQuotedColumnLines*/).build());
    }

		@Override
		public String toString() {
				if(path!=null)
						return path.toString();
				return "FileImport";
		}
}
