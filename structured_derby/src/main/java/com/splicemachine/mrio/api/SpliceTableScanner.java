/**
 * SpliceTableScanner which internally used by SpliceTableScannerBuilder
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
//import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner.KeyIndex;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;
import com.splicemachine.storage.Indexed;
import com.splicemachine.storage.index.BitIndex;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.splicemachine.utils.ByteSlice;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLDecimal;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Types;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hive.service.cli.Type;

public class SpliceTableScanner implements StandardIterator<ExecRow>{
	
	private final Timer timer;
	private final Counter filterCounter;

	private ResultScanner resultScanner;
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
	private Timer readTimer;
	private final Filter scanFilters;
    private HTable htable;
    private List<Integer> colTypes;
    private List<String> pkColNames;
    private List<Integer> pkColIds;
    private DataValueDescriptor[] data;
    
	SpliceTableScanner(ResultScanner scanner,
												ExecRow template,
												MetricFactory metricFactory,
												Scan scan,
												int[] rowDecodingMap,
												long transactionID,
												int[] allPkColumns,
												boolean[] keyColumnSortOrder,
												int[] keyColumnTypes,
												int[] keyDecodingMap,
												FormatableBitSet accessedPks,
												String indexName,
												String tableVersion, 
												HTable htable,
												List<Integer> colTypes,
												List<String> pkColNames,
												List<Integer> pkColIds) {
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
						tableVersion,null, htable,colTypes, pkColNames, pkColIds);
	}

	SpliceTableScanner(ResultScanner scanner,
												final ExecRow template,
												MetricFactory metricFactory,
												Scan scan,
												final int[] rowDecodingMap,
												final long transactionID,
												int[] keyColumnEncodingOrder,
												boolean[] keyColumnSortOrder,
												int[] keyColumnTypes,
												int[] keyDecodingMap,
												FormatableBitSet accessedPks,
												String indexName,
												final String tableVersion,
												SIFilterFactory filterFactory,
												HTable htable,
												List<Integer> colTypes,
												List<String> pkColNames,
												List<Integer> pkColIds) {
			this.scan = scan;
			this.template = template;
			this.rowDecodingMap = rowDecodingMap;
			this.keyColumnSortOrder = keyColumnSortOrder;
			this.indexName = indexName;
			this.timer = metricFactory.newTimer();
			this.filterCounter = metricFactory.newCounter();
			this.resultScanner = scanner;
			this.keyDecodingMap = keyDecodingMap;
			this.accessedKeys = accessedPks;
			
			this.keyDecoderProvider = getKeyDecoder(accessedPks, keyColumnEncodingOrder,
							keyColumnTypes, VersionedSerializers.typesForVersion(tableVersion));
			
			this.tableVersion = tableVersion;
			
			this.readTimer = metricFactory.newTimer();
			
			if(scan!=null)
				this.scanFilters = scan.getFilter();
			else
				this.scanFilters = null;
			if(filterFactory==null){
				
					this.filterFactory = new SIFilterFactory() {
							
							@SuppressWarnings("unchecked")
							public SIFilter newFilter(EntryPredicateFilter predicateFilter,
													EntryDecoder rowEntryDecoder,
													EntryAccumulator accumulator,
													boolean isCountStar) throws IOException{


									Txn baseTxn = ReadOnlyTxn.create(transactionID, Txn.IsolationLevel.SNAPSHOT_ISOLATION,null);
									TxnFilter iFilterState = HTransactorFactory.getTransactionReadController().newFilterState(null, baseTxn);

									HRowAccumulator hRowAccumulator = new HRowAccumulator(predicateFilter, getRowEntryDecoder(), accumulator, isCountStar);
									
									return new PackedTxnFilter(iFilterState, hRowAccumulator){
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
			
			this.htable = htable;
			this.colTypes = colTypes;
			this.pkColNames = pkColNames;
			this.pkColIds = pkColIds;
			
			boolean allNullFlag = true;
			if(this.template == null){
				try {
					data = createDVD();
					template.setRowArray(data);
				} catch (StandardException e) {
					// TODO Auto-generated catch block
					throw new RuntimeException("Cannot create DataValueDescriptor:"+e);
				}
				
			}
			else{
				DataValueDescriptor[] dvds = this.template.getRowArray();
				
				for(DataValueDescriptor d:dvds){
					if(d != null){
						allNullFlag = false;
						break;
					}
				}
			}
			if(allNullFlag){
				try {
					data = createDVD();
					template.setRowArray(data);
				} catch (StandardException e) {
					// TODO Auto-generated catch block
					throw new RuntimeException("Cannot create DataValueDescriptor:"+e);
				}
			}
			else
				data = template.getRowArray();	
			
	}

	private DataValueDescriptor[] createDVD() throws StandardException
	{
		DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.size()];
		for(int pos = 0; pos < colTypes.size(); pos++)
		{
			if(colTypes.get(pos) == Types.DECIMAL){	
				dvds[pos] = new SQLDecimal();
				continue;
			}
			
			dvds[pos] = DataTypeDescriptor.getBuiltInDataTypeDescriptor(colTypes.get(pos)).getNull();
		}
		return dvds;
		
	}
	
	public void open() throws StandardException, IOException {

	}
	
	public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
			
			SIFilter filter = getSIFilter();
			
			if(keyValues==null)
					keyValues = Lists.newArrayListWithExpectedSize(colTypes.size());
			boolean hasRow;
			keyValues.clear();
			
			template.resetRowArray();
			
			Result tmp = resultScanner.next();
			
			if(tmp != null)
			{
				hasRow = true;		
				
				for(KeyValue kv : tmp.raw())
				{
					keyValues.add(kv);
				}
				
			}	
			else
			{
				hasRow = false;
			}		
			
			if(hasRow)
				{					
					if(keyValues.size()<=0){
							currentRowLocation = null;	
							return null;
					}else{
						
							if(template.nColumns()>0){
								
									KeyValue kv = keyValues.get(0);
									if(!filterRowKey(kv)||!filterRow(filter)){
											//filter the row first, then filter the row key
											
											filterCounter.increment();
											setRowLocation(kv);
											
											return template;
									}
							}else if(!filterRow(filter)){
									//still need to filter rows to deal with transactional issues
								
									filterCounter.increment();
									KeyValue kv = keyValues.get(0);
									setRowLocation(kv);		
									
									return template;
							}
							
							setRowLocation(keyValues.get(0));
							
							return template;
					}
					
			}

			currentRowLocation = null;
			return null;
	}

	public RowLocation getCurrentRowLocation(){
			return currentRowLocation;
	}



	
	public void close() throws StandardException, IOException {
			if(keyAccumulator!=null)
					keyAccumulator.close();
			if(siFilter!=null)
					siFilter.getAccumulator().close();
	}

	public TimeView getTime(){
			return readTimer.getTime();
			
	}

	public long getRowsFiltered(){
			return filterCounter.getTotal();
	}

	public long getRowsVisited() {
		EntryPredicateFilter epf = getEntryPredicateFilter();
		if(epf == null)
			return getRowsOutput();
		else
			return epf.getRowsOutput()+ epf.getRowsFiltered();
	}

	private EntryPredicateFilter getEntryPredicateFilter() {
		HasPredicateFilter hpf = getReaderSIFilter();
		if(hpf == null)
			return null;
		return hpf.getFilter();
	}
	
	private HasPredicateFilter getReaderSIFilter(){
		if(scanFilters==null) return null;
		if(scanFilters instanceof FilterList){
		     FilterList fl = (FilterList) scanFilters;
		     List<Filter> filters = fl.getFilters();
	         for(Filter filter:filters){
	             if(filter instanceof HasPredicateFilter){
		             return (HasPredicateFilter) filter;
		         }
		     }
		  }else if (scanFilters instanceof HasPredicateFilter)
		     return (HasPredicateFilter) scanFilters;
	
		return null;
	}
	
	public long getRowsOutput() { return readTimer.getNumEvents(); }
	
	public void setresultScanner(ResultScanner scanner){
			this.resultScanner = scanner;
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
			if(siFilter==null){
					boolean isCountStar = scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null;
					predicateFilter= decodePredicateFilter();
					
					ExecRowAccumulator accumulator = ExecRowAccumulator.newAccumulator(predicateFilter, false, template, rowDecodingMap, tableVersion);
					
					siFilter = filterFactory.newFilter(predicateFilter,getRowEntryDecoder(),accumulator,isCountStar);
			}
			return siFilter;
	}

	protected EntryDecoder getRowEntryDecoder() {
			return new EntryDecoder();
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
			if(!isKeyed) 
			{
				return true;
			}

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

			
			public int nextSetBit(int currentPosition) {
					if(position>=allPkColumns.length)
							return -1;
					int pos =position;
					position++;
					return pos;
			}

			public boolean isScalarType(int currentPosition) {
				return sF[currentPosition]; 
			}

			public boolean isDoubleType(int currentPosition) {
				return dF[currentPosition]; 
			}

			public boolean isFloatType(int currentPosition) {
				return fF[currentPosition]; 
			}

			
			public int getPredicatePosition(int position) {
					return allPkColumns[position];
			}

			void reset(){
					position=0;
			}
	}

}
