package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.stats.*;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
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
		private KryoPool kryoPool;

		public SequentialImporter(ImportContext importContext,
															ExecRow templateRow,
															String txnId){
			this(importContext, templateRow, txnId,SpliceDriver.driver().getTableWriter(),SpliceDriver.getKryoPool());
		}

		public SequentialImporter(ImportContext importContext,
															ExecRow templateRow,
															String txnId,
															CallBufferFactory<KVPair> callBufferFactory,
															KryoPool kryoPool){
				this.importContext = importContext;
				this.rowParser = new RowParser(templateRow,importContext);
				this.kryoPool = kryoPool;

				if(importContext.shouldRecordStats()){
						metricFactory = Metrics.basicMetricFactory();
				}else
						metricFactory = Metrics.noOpMetricFactory();

				Writer.WriteConfiguration config = new ForwardingWriteConfiguration(callBufferFactory.defaultWriteConfiguration()){
						@Override
						public MetricFactory getMetricFactory() {
								return metricFactory;
						}
				};
				writeBuffer = callBufferFactory.writeBuffer(importContext.getTableName().getBytes(), txnId,config);
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
		public void processBatch(String[]... parsedRows) throws Exception {
			if(parsedRows==null) return;
			if(writeTimer==null)
					writeTimer = metricFactory.newTimer();

				writeTimer.startTiming();
				int count=0;
				for(String[] line:parsedRows){
						if(line==null) break;
						ExecRow row = rowParser.process(line,importContext.getColumnInformation());
						if(entryEncoder==null)
								entryEncoder = newEntryEncoder(row);
						writeBuffer.add(entryEncoder.encode(row));
						count++;
				}
				if(count>0)
						writeTimer.tick(count);
				else
						writeTimer.stopTiming();
		}

		@Override
		public boolean isClosed() {
				return closed;
		}

		@Override public WriteStats getWriteStats() { return writeBuffer.getWriteStats(); }
		@Override public TimeView getTotalTime() { return writeTimer.getTime(); }

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

				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null,kryoPool);

				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
		}
}
