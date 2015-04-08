package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.UUIDGenerator;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.writeconfiguration.SequentialImporterWriteConfiguration;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public class SequentialImporter implements Importer{
		private static final Logger LOG = Logger.getLogger(SequentialImporter.class);
		private final ImportContext importContext;
    private final KVPair.Type importType;
    private final MetricFactory metricFactory;

		private volatile boolean closed;
		private final RecordingCallBuffer<KVPair> writeBuffer;

		private final RowParser rowParser;

		private Timer writeTimer;
		private PairEncoder entryEncoder;

    public SequentialImporter(ImportContext importContext,
                              ExecRow templateRow,
                              TxnView txn,
                              WriteCoordinator writeCoordinator,
                              final ImportErrorReporter errorReporter,
                              KVPair.Type importType){
				this.importContext = importContext;
        this.importType = importType;
        this.rowParser = new RowParser(templateRow,importContext,errorReporter);

				if(importContext.shouldRecordStats()){
						metricFactory = Metrics.basicMetricFactory();
				}else
						metricFactory = Metrics.noOpMetricFactory();

				WriteConfiguration config = new SequentialImporterWriteConfiguration(
						writeCoordinator.defaultWriteConfiguration(), this,metricFactory, errorReporter);
							
				writeBuffer = writeCoordinator.writeBuffer(importContext.getTableName().getBytes(), txn,config);
		}

		@Override
		public void process(String[] parsedRow) throws Exception {
				processBatch(parsedRow);
		}

		public boolean isFailed(){
				return false; //by default, Sequential Imports don't record failures, since they throw errors directly
		}

		@Override
		public boolean processBatch(String[]... parsedRows) throws Exception {
			if(parsedRows==null) return false;
			if(writeTimer==null)
					writeTimer = metricFactory.newTimer();

				writeTimer.startTiming();
				SpliceLogUtils.trace(LOG,"processing %d parsed rows",parsedRows.length);
				int count=0;
				for(String[] line:parsedRows){
						if(line==null) {
								SpliceLogUtils.trace(LOG,"actually processing %d rows",count);
								break;
						}
						ExecRow row = rowParser.process(line,importContext.getColumnInformation());
						count++;
						if(row==null) continue; //unable to parse the row, so skip it.
						if(entryEncoder==null)
								entryEncoder = ImportUtils.newEntryEncoder(row,importContext,getRandomGenerator(),importType);
						// Don't write any rows if we are just doing a pre-import ("check") scan.
						if(!importContext.isCheckScan())
							writeBuffer.add(entryEncoder.encode(row));
				}
				if(count>0)
						writeTimer.tick(count);
				else
						writeTimer.stopTiming();
				return count==parsedRows.length; //return true if the batch was full
		}

		@Override
		public boolean isClosed() {
				return closed;
		}

		@Override public WriteStats getWriteStats() {
				if(writeBuffer==null) return WriteStats.NOOP_WRITE_STATS;
				return writeBuffer.getWriteStats();
		}
		@Override
		public TimeView getTotalTime() {
				if(writeTimer==null) return Metrics.noOpTimeView();
				return writeTimer.getTime();
		}

		@Override
		public void close() throws IOException {
				closed=true;
				if(writeTimer==null) return; //we never wrote any records, so don't bother closing
				try {
						writeTimer.startTiming();
						writeBuffer.close();
						writeTimer.stopTiming();
				} catch (Exception e) {
						throw new IOException(e);
				}finally{
						Closeables.closeQuietly(entryEncoder);
				}
		}

		protected UUIDGenerator getRandomGenerator(){
				return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
		}

}
