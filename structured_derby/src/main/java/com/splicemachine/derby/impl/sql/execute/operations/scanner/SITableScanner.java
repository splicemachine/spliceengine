package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner implements StandardIterator<ExecRow>{
	private static Logger LOG = Logger.getLogger(SITableScanner.class);
		private final Timer timer;
		private final Counter filterCounter;

		private MeasuredRegionScanner regionScanner;
		private final Scan scan;
		private final ExecRow template;
		private final String tableVersion;
		private final int[] rowDecodingMap;


		private SIFilter siFilter;
		private EntryPredicateFilter predicateFilter;
		private List<KeyValue> keyValues;

		private RowLocation currentRowLocation;
		private final boolean[] keyColumnSortOrder;
		private String indexName;
		private ByteSlice slice = new ByteSlice();

		private boolean isKeyed = true;
		private KeyIndex primaryKeyIndex;
		private MultiFieldDecoder keyDecoder;
		private final Supplier<MultiFieldDecoder> keyDecoderProvider;
		private ExecRowAccumulator keyAccumulator;
		private int[] keyDecodingMap;
		private FormatableBitSet accessedKeys;

		private final SIFilterFactory filterFactory;
    private ExecRowAccumulator accumulator;


    SITableScanner(MeasuredRegionScanner scanner,
													ExecRow template,
													MetricFactory metricFactory,
													Scan scan,
													int[] rowDecodingMap,
													String transactionID,
													int[] allPkColumns,
													boolean[] keyColumnSortOrder,
													int[] keyColumnTypes,
													int[] keyDecodingMap,
													FormatableBitSet accessedPks,
													String indexName,
													String tableVersion) {
			this(scanner,
							template,
							metricFactory,
							scan,
							rowDecodingMap,
							transactionID,
							allPkColumns,
							keyColumnSortOrder,
							keyColumnTypes,
							keyDecodingMap,
							accessedPks,
							indexName,
							tableVersion,null);
		}

		SITableScanner(MeasuredRegionScanner scanner,
													final ExecRow template,
													MetricFactory metricFactory,
													Scan scan,
													final int[] rowDecodingMap,
													final String transactionID,
													int[] keyColumnEncodingOrder,
													boolean[] keyColumnSortOrder,
													int[] keyColumnTypes,
													int[] keyDecodingMap,
													FormatableBitSet accessedPks,
													String indexName,
													final String tableVersion,
													SIFilterFactory filterFactory) {
				this.scan = scan;
				this.template = template;
				this.rowDecodingMap = rowDecodingMap;
				this.keyColumnSortOrder = keyColumnSortOrder;
				this.indexName = indexName;
				this.timer = metricFactory.newTimer();
				this.filterCounter = metricFactory.newCounter();
				this.regionScanner = scanner;
				this.keyDecodingMap = keyDecodingMap;
				this.accessedKeys = accessedPks;
				this.keyDecoderProvider = getKeyDecoder(accessedPks, keyColumnEncodingOrder,
								keyColumnTypes, VersionedSerializers.typesForVersion(tableVersion));
				this.tableVersion = tableVersion;
				if(filterFactory==null){
						this.filterFactory = new SIFilterFactory() {
								@Override
								public SIFilter newFilter(EntryPredicateFilter predicateFilter,
																					EntryDecoder rowEntryDecoder,
																					EntryAccumulator accumulator,
																					boolean isCountStar) throws IOException {
										TransactionId transactionId= new TransactionId(transactionID);
										RollForwardQueue queue = RollForwardQueueMap.lookupRollForward(regionScanner.getRegionInfo().getRegionNameAsString());
										if (queue == null)
											SpliceLogUtils.warn(LOG, "SI Table Scanner is not rolling forward, configuration issue");

										IFilterState iFilterState = HTransactorFactory.getTransactionReadController().newFilterState(queue, transactionId);

										HRowAccumulator hRowAccumulator = new HRowAccumulator(predicateFilter, getRowEntryDecoder(), accumulator, isCountStar);
										//noinspection unchecked
										return new FilterStatePacked((FilterState)iFilterState, hRowAccumulator){
												@Override
												public Filter.ReturnCode doAccumulate(KeyValue dataKeyValue) throws IOException {
														if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
																if (!accumulator.accumulate(dataKeyValue)) {
																		return Filter.ReturnCode.NEXT_ROW;
																}
																return Filter.ReturnCode.INCLUDE;
														}else return Filter.ReturnCode.INCLUDE;
												}
										};
								}
						};
				}
				else
						this.filterFactory = filterFactory;
		}

		@Override
		public void open() throws StandardException, IOException {

		}

		@Override
		public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

				SIFilter filter = getSIFilter();
				if(keyValues==null)
						keyValues = Lists.newArrayListWithExpectedSize(2);
				boolean hasRow;
				do{
						keyValues.clear();
						template.resetRowArray(); //necessary to deal with null entries--maybe make the underlying call faster?
						hasRow = regionScanner.next(keyValues);
						if(keyValues.size()<=0){
								currentRowLocation = null;
								return null;
						}else{
                                updatePredicateFilterIfNecessary(keyValues.get(0));
								if(template.nColumns()>0){
										if(!filterRowKey(keyValues.get(0))||!filterRow(filter)){
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
								return template;
						}
				}while(hasRow);

				currentRowLocation = null;
				return null;
		}

		public RowLocation getCurrentRowLocation(){
				return currentRowLocation;
		}


		//TODO -sf- add a nextBatch() method

		@Override
		public void close() throws StandardException, IOException {
				if(keyAccumulator!=null)
						keyAccumulator.close();
				if(siFilter!=null)
						siFilter.getAccumulator().close();
		}

		public TimeView getTime(){
				return regionScanner.getReadTime();
		}

		public long getRowsFiltered(){
				return filterCounter.getTotal();
		}

		public long getRowsVisited() {
				return regionScanner.getRowsVisited();
		}

		public void setRegionScanner(MeasuredRegionScanner scanner){
				this.regionScanner = scanner;
		}


    public long getBytesVisited() {
        return regionScanner.getBytesVisited();
    }

    /*********************************************************************************************************************/
		/*Private helper methods*/
		private Supplier<MultiFieldDecoder> getKeyDecoder(FormatableBitSet accessedPks,
																											int[] allPkColumns,
																											int[] keyColumnTypes,
																											TypeProvider typeProvider) {
				if(accessedPks==null||accessedPks.getNumBitsSet()<=0){
						isKeyed = false;
						return null;
				}

				primaryKeyIndex = getIndex(allPkColumns,keyColumnTypes,typeProvider);

				keyDecoder = MultiFieldDecoder.create();
				return Suppliers.ofInstance(keyDecoder);
		}

		private KeyIndex getIndex(final int[] allPkColumns, int[] keyColumnTypes,TypeProvider typeProvider) {
			return new KeyIndex(allPkColumns,keyColumnTypes, typeProvider);
		}

		@SuppressWarnings("unchecked")
		private SIFilter getSIFilter() throws IOException {
				if(siFilter==null) {
					boolean isCountStar = scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null;
                    predicateFilter= buildInitialPredicateFilter();
					accumulator = ExecRowAccumulator.newAccumulator(predicateFilter, false, template, rowDecodingMap, tableVersion);
					siFilter = filterFactory.newFilter(predicateFilter,getRowEntryDecoder(),accumulator,isCountStar);
				}
				return siFilter;
		}

    private void updatePredicateFilterIfNecessary(KeyValue kv) throws IOException {
        Filter filter = scan.getFilter();
        if(filter instanceof SkippingScanFilter){
            SkippingScanFilter ssF = (SkippingScanFilter)filter;
            predicateFilter.setValuePredicates(ssF.getNextPredicates(kv));
        }else if(filter instanceof FilterList){
            FilterList fl = (FilterList) filter;
            for(Filter f:fl.getFilters()){
                if(f instanceof SkippingScanFilter){
                    SkippingScanFilter ssF = (SkippingScanFilter)f;
                    predicateFilter.setValuePredicates(ssF.getNextPredicates(kv));
                }
            }
        }
    }

    private boolean scanHasSkippingScanFilter() throws IOException {
        Filter filter = scan.getFilter();
        if(filter instanceof SkippingScanFilter){
            return true;
        }else if(filter instanceof FilterList){
            FilterList fl = (FilterList) filter;
            for(Filter f:fl.getFilters()){
                if(f instanceof SkippingScanFilter){
                    return true;
                }
            }
        }
        return false;
    }


    protected EntryDecoder getRowEntryDecoder() {
				return new EntryDecoder();
		}

	private EntryPredicateFilter buildInitialPredicateFilter() throws IOException {
        EntryPredicateFilter entryPredicateFilter = EntryPredicateFilter.fromBytes(scan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL));
        BitSet checkedColumns = entryPredicateFilter.getCheckedColumns();
        if(scanHasSkippingScanFilter()){
                /*
                 * We have to be careful--EMPTY_PREDICATE could have been returned, in which case
                 * setting predicates can cause all kinds of calamituous behavior. To avoid that, when
                 * we have a skipscan filter (e.g. some kind of block scanning like a Batch NLJ) and we
                 * have an empty Predicate Filter to begin with, then we clone the predicate filter to a new
                 * one to avoid contamination.
                 *
                 */
            return new EntryPredicateFilter(checkedColumns,new ObjectArrayList<Predicate>(),entryPredicateFilter.indexReturned());
        }
        return entryPredicateFilter;
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

		private boolean filterRow(SIFilter filter) throws IOException {
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
				return keyValues.size() > 0 && filter.getAccumulator().result() != null;
		}

		private boolean filterRowKey(KeyValue keyValue) throws IOException {
				if(!isKeyed) return true;

				byte[] dataBuffer = keyValue.getBuffer();
				int dataOffset = keyValue.getRowOffset();
				int dataLength = keyValue.getRowLength();
				keyDecoder.set(dataBuffer, dataOffset, dataLength);
				if(keyAccumulator==null)
						keyAccumulator = ExecRowAccumulator.newAccumulator(predicateFilter,false,template,
																keyDecodingMap, keyColumnSortOrder, accessedKeys, tableVersion);

				keyAccumulator.reset();
				primaryKeyIndex.reset();

				return predicateFilter.match(primaryKeyIndex, keyDecoderProvider, keyAccumulator);
		}

    private class KeyIndex implements Indexed{
				private final int[] allPkColumns;
				private final int[] keyColumnTypes;
				private final TypeProvider typeProvider;
				private int position = 0;
				
				private final boolean[] sF;
				private final boolean[] fF;
				private final boolean[] dF;

				private KeyIndex(int[] allPkColumns,
												 int[] keyColumnTypes,
												 TypeProvider typeProvider) {
						this.allPkColumns = allPkColumns;
						this.keyColumnTypes = keyColumnTypes;
						this.typeProvider = typeProvider;
						sF = new boolean[keyColumnTypes.length];
						fF = new boolean[keyColumnTypes.length];
						dF = new boolean[keyColumnTypes.length];
						
						for(int i=0;i<sF.length;i++){
							sF[i] = typeProvider.isScalar(keyColumnTypes[i]);
							fF[i] = typeProvider.isFloat(keyColumnTypes[i]);
							dF[i] = typeProvider.isDouble(keyColumnTypes[i]);							
						}
				}

				@Override public int nextSetBit(int currentPosition) {
						if(position>=allPkColumns.length)
								return -1;
						int pos =position;
						position++;
						return pos;
				}

				@Override public boolean isScalarType(int currentPosition) {
					return sF[currentPosition]; 
				}

				@Override public boolean isDoubleType(int currentPosition) {
					return dF[currentPosition]; 
				}

				@Override public boolean isFloatType(int currentPosition) {
					return fF[currentPosition]; 
				}

				@Override
				public int getPredicatePosition(int position) {
						return allPkColumns[position];
				}

				void reset(){
						position=0;
				}
		}

}
