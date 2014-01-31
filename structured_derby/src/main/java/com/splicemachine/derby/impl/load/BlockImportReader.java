package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.stats.*;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class BlockImportReader implements ImportReader {
    private static final long serialVersionUID = 1l;

    private BlockLocation location;

    private CSVParser parser;

		private Text text;
		private LongWritable pos; //unused by the app
		private LineRecordReader lineRecordReader;

		private Timer timer;
		private long initialPos;
		private long finalPos;

		@Deprecated
    public BlockImportReader() { }

    public BlockImportReader(BlockLocation location, boolean shouldRecordStats) {
        this.location = location;
				timer = shouldRecordStats? Metrics.newTimer(): Metrics.noOpTimer();
		}

    @Override
    public String[] nextRow() throws IOException {
				timer.startTiming();
				if(lineRecordReader.next(pos,text)){
						String line = text.toString();
						if(line==null||line.length()==0){
								timer.stopTiming();
								return null; //may have reached the end of the file without finding a record
						}
						timer.tick(1);
						return parser.parseLine(line);
				}else{
						timer.stopTiming();
						return null;
				}
    }

    @Override
    public void setup(FileSystem fileSystem, ImportContext importContext) throws IOException{
				FileSplit split = new FileSplit(importContext.getFilePath(),
								location.getOffset(),
								location.getLength(),
								location.getHosts());

				lineRecordReader = new LineRecordReader(SpliceConstants.config,split);
				//initialize base variables
				text = new Text();
				pos = new LongWritable(location.getOffset());

        parser = getCsvParser(importContext);
				initialPos = lineRecordReader.getPos();
    }

		@Override
		public IOStats getStats() {
				if(timer.getNumEvents()<=0) return Metrics.noOpIOStats();
				return new BaseIOStats(timer.getTime(),(finalPos-initialPos),timer.getNumEvents());
		}

		@Override
    public void close() throws IOException {
				finalPos = lineRecordReader.getPos();
				lineRecordReader.close();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(location.getHosts().length);
        for (String host: location.getHosts())
            out.writeUTF(host);
        out.writeInt(location.getNames().length);
        for (String name: location.getNames())
            out.writeUTF(name);
        out.writeInt(location.getTopologyPaths().length);
        for (String topologyPath: location.getTopologyPaths())
            out.writeUTF(topologyPath);
        out.writeLong(location.getOffset());
        out.writeLong(location.getLength());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        location = new BlockLocation();
        String[] hosts = new String[in.readInt()];
        for (int j = 0; j<hosts.length; j++) {
            hosts[j] = in.readUTF();
        }
        String[] names = new String[in.readInt()];
        for (int j = 0; j<names.length; j++) {
            names[j] = in.readUTF();
        }
        String[] topologyPaths = new String[in.readInt()];
        for (int j = 0; j<topologyPaths.length; j++) {
            topologyPaths[j] = in.readUTF();
        }
        location.setHosts(hosts);
        location.setNames(names);
        location.setTopologyPaths(topologyPaths);
        location.setOffset(in.readLong());
        location.setLength(in.readLong());
    }

		@Override
		public boolean shouldParallelize(FileSystem fs, ImportContext ctx) throws IOException {
				return false;
		}

		BlockLocation getLocation(){
				//exposed for testing purposes
				return location;
		}

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(context.getColumnDelimiter().charAt(0),context.getQuoteChar().charAt(0));
    }
}
