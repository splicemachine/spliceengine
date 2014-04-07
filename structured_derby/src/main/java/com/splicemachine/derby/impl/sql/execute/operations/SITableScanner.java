package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Indexed;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Provider;
import com.splicemachine.utils.Providers;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner implements StandardIterator<ExecRow>{
		private final Timer timer;
		private final Counter filterCounter;

		private RegionScanner regionScanner;
		private final Scan scan;
		private final ExecRow template;

		private final Provider<MultiFieldDecoder> keyDecoderProvider;
		private MultiFieldDecoder keyDecoder;

		private final String transactionID;
		private final int[] rowColumnMap;

		private FilterStatePacked siFilter;

		private EntryPredicateFilter predicateFilter;
		private List<KeyValue> keyValues;
		private RowLocation currentRowLocation;
		private String indexName;
		private ByteSlice slice = new ByteSlice();
		private boolean hasPks = true;
		private Indexed primaryKeyIndex;
		private ExecRowAccumulator accumulator;
		private final String tableVersion;
		private ExecRowAccumulator keyAccumulator;
		private int[] pkColumns;

		public SITableScanner(RegionScanner scanner,
													ExecRow template,
													SpliceRuntimeContext context,
													Scan scan,
													int[] rowColumnMap,
													String transactionID,
													int[] pkColumns,
													FormatableBitSet accessedPkColumns,
													String indexName,
													String tableVersion) {
				this.scan = scan;
				this.template = template;
				this.transactionID = transactionID;
				this.indexName = indexName;
				this.timer = context.newTimer();
				this.filterCounter = context.newCounter();
				this.regionScanner = scanner;
				this.pkColumns = pkColumns;
				this.keyDecoderProvider = getKeyDecoder(template, accessedPkColumns);
				this.rowColumnMap = rowColumnMap;
				this.tableVersion = tableVersion;
		}

		@Override
		public void open() throws StandardException, IOException {

		}

		@Override
		public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				timer.startTiming();

				FilterStatePacked filter = getSIFilter();
				if(keyValues==null)
						keyValues = Lists.newArrayListWithExpectedSize(2);
				boolean hasRow;
				do{
						keyValues.clear();
						hasRow = regionScanner.next(keyValues);
						if(keyValues.size()<=0){
								currentRowLocation = null;
								return null;
						}else{
								template.resetRowArray();
								if(template.nColumns()>0){
										if(!filterRow(filter)||!filterRowKey(keyValues.get(0))){
												//filter the row first, then filter the row key
												filterCounter.increment();
												continue;
										}
								}else if(!filterRow(filter)){
										//still need to filter rows to deal with transactional issues
										filterCounter.increment();
										continue;
								}
								setRowLocation(keyValues.get(0));
								break;
						}
				}while(hasRow);
				if(!hasRow){
						timer.stopTiming();
						currentRowLocation = null;
						return null;
				}else
						timer.tick(1);
				return template;
		}

		public RowLocation getCurrentRowLocation(){
				return currentRowLocation;
		}


		//TODO -sf- add a nextBatch() method

		@Override
		public void close() throws StandardException, IOException {

		}

		public TimeView getTime(){
				return timer.getTime();
		}

		public long getRowsFiltered(){
				return filterCounter.getTotal();
		}

		public long getRowsVisited() {
				return timer.getNumEvents();
		}

		public void setRegionScanner(RegionScanner scanner){
				this.regionScanner = scanner;
		}

/*********************************************************************************************************************/
		/*Private helper methods*/
		private Provider<MultiFieldDecoder> getKeyDecoder(ExecRow template, FormatableBitSet pkColumns) {
				if(pkColumns==null||pkColumns.getNumBitsSet()<=0){
						hasPks = false;
						return null;
				}

				primaryKeyIndex = getIndex(pkColumns,template);

				keyDecoder = MultiFieldDecoder.create(SpliceKryoRegistry.getInstance());
				return Providers.basicProvider(keyDecoder);
		}

		private Indexed getIndex(final FormatableBitSet pkColumns, final ExecRow template) {
				return new Indexed() {
						@Override public int nextSetBit(int currentPosition) { return pkColumns.anySetBit(currentPosition-1); }

						@Override public boolean isScalarType(int position) {
								return DerbyBytesUtil.isScalarType(template.getRowArray()[rowColumnMap[position]], null);
						}

						@Override public boolean isDoubleType(int position) {
								return DerbyBytesUtil.isDoubleType(template.getRowArray()[rowColumnMap[position]]);
						}

						@Override public boolean isFloatType(int position) {
								return DerbyBytesUtil.isFloatType(template.getRowArray()[rowColumnMap[position]]);
						}
				};
		}

		@SuppressWarnings("unchecked")
		private FilterStatePacked getSIFilter() throws IOException {
				if(siFilter==null){
						boolean isCountStar = scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null;
						predicateFilter= decodePredicateFilter();
						TransactionId txnId= new TransactionId(this.transactionID);
						IFilterState iFilterState = HTransactorFactory.getTransactionReadController().newFilterState(null, txnId);
						accumulator = ExecRowAccumulator.newAccumulator(predicateFilter, false, template, rowColumnMap, tableVersion);

						HRowAccumulator hRowAccumulator = new HRowAccumulator(predicateFilter, getRowEntryDecoder(), accumulator, isCountStar);
						siFilter = new FilterStatePacked((FilterState)iFilterState, hRowAccumulator){
								@Override
								protected Filter.ReturnCode doAccumulate(KeyValue dataKeyValue) throws IOException {
										if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
												if (!accumulator.accumulate(dataKeyValue)) {
														return Filter.ReturnCode.NEXT_ROW;
												}
												return Filter.ReturnCode.INCLUDE;
										}else return Filter.ReturnCode.INCLUDE;
								}
						};
				}
				return siFilter;
		}

		protected EntryDecoder getRowEntryDecoder() {
				return new EntryDecoder(SpliceKryoRegistry.getInstance());
		}

		private EntryPredicateFilter decodePredicateFilter() throws IOException {
				return EntryPredicateFilter.fromBytes(scan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL));
		}

		protected void setRowLocation(KeyValue sampleKv) throws StandardException {
				if(indexName!=null && template.nColumns() > 0 && template.getColumn(template.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
					 /*
						* If indexName !=null, then we are currently scanning an index,
						* so our RowLocation should point to the main table, and not to the
						* index (that we're actually scanning)
						*/
						currentRowLocation = (RowLocation) template.getColumn(template.nColumns());
				} else {
						slice.set(sampleKv.getBuffer(), sampleKv.getRowOffset(), sampleKv.getRowLength());
						if(currentRowLocation==null)
								currentRowLocation = new HBaseRowLocation(slice);
						else
								currentRowLocation.setValue(slice);
				}
		}

		private boolean filterRow(FilterStatePacked filter) throws IOException {
				filter.nextRow();
				Iterator<KeyValue> kvIter = keyValues.iterator();
				while(kvIter.hasNext()){
						KeyValue kv = kvIter.next();
						Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
						switch(returnCode){
								case NEXT_COL:
								case NEXT_ROW:
								case SEEK_NEXT_USING_HINT:
										return false; //failed the predicate
								case SKIP:
										kvIter.remove();
								default:
										//these are okay--they mean the encoding is good
						}
				}
				return filter.getAccumulator().result() != null && keyValues.size()>0;
		}

		private boolean filterRowKey(KeyValue keyValue) throws IOException {
				if(!hasPks) return true;

				byte[] dataBuffer = keyValue.getBuffer();
				int dataOffset = keyValue.getRowOffset();
				int dataLength = keyValue.getRowLength();
				keyDecoder.set(dataBuffer,dataOffset,dataLength);
				if(keyAccumulator==null)
						keyAccumulator = ExecRowAccumulator.newAccumulator(predicateFilter,false,template,pkColumns,tableVersion);

				keyAccumulator.reset();
				if(!predicateFilter.match(primaryKeyIndex,keyDecoderProvider,accumulator))
						keyAccumulator.reset();

				return predicateFilter.match(primaryKeyIndex, keyDecoderProvider,accumulator);
		}

}
