package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.SameThreadExecutorService;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpKeyHashDecoder;
import com.splicemachine.derby.utils.marshall.SkippingKeyDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/11/14
 */
public class IndexRowReaderBuilder {
		private SpliceOperation sourceOperation;
		private int lookupBatchSize;
		private int numConcurrentLookups = -1;
		private ExecRow  outputTemplate;
		private String txnId;
		private long mainTableConglomId = -1;

		private int[] mainTableRowDecodingMap;
		private FormatableBitSet mainTableAccessedRowColumns;

		private int[] mainTableKeyColumnEncodingOrder;
		private boolean[] mainTableKeyColumnSortOrder;
		private int[] mainTableKeyDecodingMap;
		private FormatableBitSet mainTableAccessedKeyColumns;

		/*
		 * A Map from the physical location of the Index columns in the INDEX scanned row
		 * and the decoded output row.
		 */
		private int[] indexCols;
		private MetricFactory metricFactory;
		private SpliceRuntimeContext runtimeContext;
		private String tableVersion;
		private int[] mainTableKeyColumnTypes;
		private HTableInterface table;

		public IndexRowReaderBuilder indexColumns(int[] indexCols){
				this.indexCols = indexCols;
				return this;
		}

		public IndexRowReaderBuilder mainTableVersion(String mainTableVersion){
				this.tableVersion = mainTableVersion;
				return this;
		}

		public IndexRowReaderBuilder source(SpliceOperation sourceOperation) {
				this.sourceOperation = sourceOperation;
				return this;
		}

		public IndexRowReaderBuilder lookupBatchSize(int lookupBatchSize) {
				this.lookupBatchSize = lookupBatchSize;
				return this;
		}

		public IndexRowReaderBuilder numConcurrentLookups(int numConcurrentLookups) {
				this.numConcurrentLookups = numConcurrentLookups;
				return this;
		}

		public IndexRowReaderBuilder outputTemplate(ExecRow outputTemplate) {
				this.outputTemplate = outputTemplate;
				return this;
		}

		public IndexRowReaderBuilder transactionId(String txnId) {
				this.txnId = txnId;
				return this;
		}

		public IndexRowReaderBuilder mainTableConglomId(long mainTableConglomId) {
				this.mainTableConglomId = mainTableConglomId;
				return this;
		}

		public IndexRowReaderBuilder mainTableAccessedRowColumns(FormatableBitSet mainTableAccessedRowColumns){
				this.mainTableAccessedRowColumns = mainTableAccessedRowColumns;
				return this;
		}

		public IndexRowReaderBuilder mainTableRowDecodingMap(int[] mainTableRowDecodingMap) {
				this.mainTableRowDecodingMap = mainTableRowDecodingMap;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnEncodingOrder(int[] mainTableKeyColumnEncodingOrder) {
				this.mainTableKeyColumnEncodingOrder = mainTableKeyColumnEncodingOrder;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnTypes(int[] mainTableKeyColumnTypes) {
				this.mainTableKeyColumnTypes = mainTableKeyColumnTypes;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyColumnSortOrder(boolean[] mainTableKeyColumnSortOrder) {
				this.mainTableKeyColumnSortOrder = mainTableKeyColumnSortOrder;
				return this;
		}

		public IndexRowReaderBuilder mainTableKeyDecodingMap(int[] mainTableKeyDecodingMap) {
				this.mainTableKeyDecodingMap = mainTableKeyDecodingMap;
				return this;
		}

		public IndexRowReaderBuilder mainTableAccessedKeyColumns(FormatableBitSet mainTableAccessedKeyColumns) {
				this.mainTableAccessedKeyColumns = mainTableAccessedKeyColumns;
				return this;
		}

		public IndexRowReaderBuilder metricFactory(MetricFactory metricFactory) {
				this.metricFactory = metricFactory;
				return this;
		}

		public IndexRowReaderBuilder runtimeContext(SpliceRuntimeContext runtimeContext) {
				this.runtimeContext = runtimeContext;
				return this;
		}

		public IndexRowReaderBuilder table(HTableInterface table) {
				this.table = table;
				return this;
		}

		public IndexRowReader build() throws StandardException{
				assert txnId!=null: "No Transaction specified!";
				assert runtimeContext!=null: "No Runtime context specified!";
				assert mainTableRowDecodingMap!=null: "No Row decoding map specified!";
				assert mainTableConglomId>=0: "No Main table conglomerate id specified!";
				assert outputTemplate!=null: "No output template specified!";
				assert sourceOperation!=null: "No source specified";
				assert indexCols!=null: "No index columns specified!";

				if(metricFactory==null)
						metricFactory = Metrics.noOpMetricFactory();
				ExecutorService lookupService;
				if(numConcurrentLookups<0)
						lookupService = SameThreadExecutorService.instance();
				else{
						ThreadFactory factory = new ThreadFactoryBuilder()
										.setNameFormat("index-lookup-%d").build();
						lookupService = new ThreadPoolExecutor(numConcurrentLookups,numConcurrentLookups,
										60, TimeUnit.SECONDS,new SynchronousQueue<Runnable>(),factory,
										new ThreadPoolExecutor.CallerRunsPolicy());
				}

				BitSet rowFieldsToReturn = new BitSet(mainTableAccessedRowColumns.getNumBitsSet());
				for(int i=mainTableAccessedRowColumns.anySetBit();i>=0;i=mainTableAccessedRowColumns.anySetBit(i)){
						rowFieldsToReturn.set(i);
				}
				EntryPredicateFilter epf = new EntryPredicateFilter(rowFieldsToReturn, ObjectArrayList.<Predicate>newInstanceWithCapacity(0));
				byte[] epfBytes = epf.toBytes();

				DescriptorSerializer[] templateSerializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(outputTemplate);
				KeyHashDecoder keyDecoder;
				if(mainTableKeyColumnEncodingOrder==null || mainTableAccessedKeyColumns==null||mainTableAccessedKeyColumns.getNumBitsSet()<=0)
						keyDecoder = NoOpKeyHashDecoder.INSTANCE;
				else{
						keyDecoder = SkippingKeyDecoder.decoder(VersionedSerializers.typesForVersion(tableVersion),
										templateSerializers,
										mainTableKeyColumnEncodingOrder,
										mainTableKeyColumnTypes,
										mainTableKeyColumnSortOrder,
										mainTableKeyDecodingMap,
										mainTableAccessedKeyColumns);
				}

				KeyHashDecoder rowDecoder = new EntryDataDecoder(mainTableRowDecodingMap,null,templateSerializers);

				return new IndexRowReader(lookupService,
								sourceOperation,
								outputTemplate,
								txnId,
								lookupBatchSize,
								Math.max(numConcurrentLookups,0),
								mainTableConglomId,
								epfBytes,
								metricFactory,
								runtimeContext,
								keyDecoder,
								rowDecoder,
								indexCols,table);
		}

}
