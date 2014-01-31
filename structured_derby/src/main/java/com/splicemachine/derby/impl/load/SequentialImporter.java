package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.stats.*;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public class SequentialImporter implements Importer{
		private final ImportContext importContext;
		private final MetricFactory metricFactory;

		private volatile boolean closed;
		private final RecordingCallBuffer<KVPair> writeBuffer;

		private final RowParser rowParser;

		private Timer writeTimer;
		private PairEncoder entryEncoder;

		public SequentialImporter(ImportContext importContext,
															ExecRow templateRow,
															String txnId){
			this(importContext, templateRow, txnId,SpliceDriver.driver().getTableWriter());
		}

		public SequentialImporter(ImportContext importContext,
															ExecRow templateRow,
															String txnId,
															CallBufferFactory<KVPair> callBufferFactory){
				this.importContext = importContext;
				this.rowParser = new RowParser(templateRow,importContext);

				writeBuffer = callBufferFactory.writeBuffer(importContext.getTableName().getBytes(), txnId);
				if(importContext.shouldRecordStats()){
						metricFactory = Metrics.basicMetricFactory();
				}else
						metricFactory = Metrics.noOpMetricFactory();
		}

		@Override
		public void process(String[] parsedRow) throws Exception {
				if(writeTimer==null){
						writeTimer = metricFactory.newTimer();
				}

				ExecRow newRow = rowParser.process(parsedRow,importContext.getColumnInformation());
				if(entryEncoder==null)
						entryEncoder = newEntryEncoder(newRow);

				writeBuffer.add(entryEncoder.encode(newRow));
				writeTimer.tick(1);
		}


		@Override
		public boolean isClosed() {
				return closed;
		}

		@Override
		public IOStats getStats() {
				long bytesWritten = writeBuffer.getTotalBytesAdded();
				long rowsWritten = writeBuffer.getTotalElementsAdded();
				TimeView writeTime = writeTimer.getTime();
				return new BaseIOStats(writeTime,bytesWritten,rowsWritten);
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
				}
		}

		protected Snowflake.Generator getRandomGenerator(){
				return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
		}

		private PairEncoder newEntryEncoder(ExecRow row) {
				int[] pkCols = importContext.getPrimaryKeys();

				KeyEncoder encoder;
				if(pkCols!=null&& pkCols.length>0){
						encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(pkCols, null), NoOpPostfix.INSTANCE);
				}else
						encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);

				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null);

				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
		}
}
