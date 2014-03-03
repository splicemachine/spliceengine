package com.splicemachine.derby.utils;

import java.io.IOException;
import java.util.Comparator;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.operations.QualifierUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.filter.ColumnNullableFilter;
import com.splicemachine.storage.AndPredicate;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.NullPredicate;
import com.splicemachine.storage.OrPredicate;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.ValuePredicate;

/**
 * Utility methods and classes related to building HBase Scans
 * @author jessiezhang
 * @author johnleach
 * @author Scott Fines
 * Created: 1/24/13 10:50 AM
 */
public class Scans extends SpliceUtils {
		private static Comparator<? super Qualifier> qualifierComparator = new Comparator<Qualifier>() {
				@Override
				public int compare(Qualifier o1, Qualifier o2) {
						if(o1==null){
								if(o2==null) return 0;
								return -1;
						}
						else if(o2==null) return 1;
						else
								return o1.getColumnId() - o2.getColumnId();

				}
		};

		private Scans(){} //can't construct me



		/**
		 * Builds a correct scan to scan the range of a table which has the prefix specified by {@code prefix}.
		 *
		 * In essence, this creates a start key which is the prefix, and an end key which is a lexicographic
		 * increment of the prefix, so that all data which is lexicographically between those values will
		 * be returned by the scan.
		 *
		 * @param prefix the prefix to search
		 * @param transactionId the transaction ID
		 * @return a transactionally-aware Scan which will scan all keys stored lexicographically between
		 * {@code prefix} and the lexicographic increment of {@code prefix}
		 * @throws IOException if {@code prefix} is unable to be correctly converted into a byte[]
		 */
		public static Scan buildPrefixRangeScan(DataValueDescriptor prefix,String transactionId) throws IOException {
				try {
						byte[] start = DerbyBytesUtil.generateBeginKeyForTemp(prefix);
						byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
						return newScan(start,finish,transactionId);
				} catch (StandardException e) {
						throw new IOException(e);
				}
		}

		/**
		 * Builds a correct scan to scan the range of a table which has the prefix specified by {@code prefix}.
		 *
		 * In essence, this creates a start key which is the prefix, and an end key which is a lexicographic
		 * increment of the prefix, so that all data which is lexicographically between those values will
		 * be returned by the scan.
		 *
		 * @param prefix the prefix to search
		 * @param transactionId the transaction ID
		 * @return a transactionally-aware Scan which will scan all keys stored lexicographically between
		 * {@code prefix} and the lexicographic increment of {@code prefix}
		 * @throws IOException if {@code prefix} is unable to be correctly converted into a byte[]
		 */
		public static Scan buildPrefixRangeScan(byte[] prefix,String transactionId) throws IOException {
				byte[] start = new byte[prefix.length];
				System.arraycopy(prefix,0,start,0,start.length);

				byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
				return newScan(start,finish,transactionId);
		}

		/**
		 * Convenience utility for calling {@link #newScan(byte[], byte[], String, int)} when
		 * the Default cache size is acceptable and the transactionID is a string.
		 *
		 * @param start the start row of the scan
		 * @param finish the stop row of the scan
		 * @param transactionID the transactionID.
		 * @return a transactionally-aware scan constructed with {@link #DEFAULT_CACHE_SIZE}
		 */
		public static Scan newScan(byte[] start, byte[] finish, String transactionID) {
				return newScan(start, finish, transactionID, DEFAULT_CACHE_SIZE);
		}
		/**
		 * Constructs a new Scan from the specified start and stop rows, and adds transaction information
		 * correctly.
		 *
		 * @param startRow the start of the scan
		 * @param stopRow the end of the scan
		 * @param transactionId the transaction id for the scan
		 * @param caching the number of rows to cache.
		 * @return a correctly constructed, transactionally-aware scan.
		 */
		public static Scan newScan(byte[] startRow, byte[] stopRow,
															 String transactionId, int caching) {
				Scan scan = SpliceUtils.createScan(transactionId);
				scan.setCaching(caching);
				scan.setStartRow(startRow);
				scan.setStopRow(stopRow);
				scan.addFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);
				return scan;
		}

		/**
		 * Builds a Scan from qualified starts and stops.
		 *
		 * This method does the following:
		 *
		 * 1. builds a basic scan with {@link #DEFAULT_CACHE_SIZE} and attaches transaction information to it.
		 * 2. Constructs start and stop keys for the scan based on {@code startKeyValue} and {@code stopKeyValue},
		 * according to the following rules:
		 * 		A. if {@code startKeyValue ==null}, then set "" as the start of the scan
		 * 	 	B. if {@code startKeyValue !=null}, then serialize the startKeyValue into a start key and set that.
		 * 	 	C. if {@code stopKeyValue ==null}, then set "" as the end of the scan
		 * 	 	D. if {@code stopKeyValue !=null}, then serialize the stopKeyValue into a stop key and set that.
		 * 3. Construct startKeyFilters as necessary, according to the rules defined in
		 * {@link #buildKeyFilter(org.apache.derby.iapi.types.DataValueDescriptor[],
		 * 												int, org.apache.derby.iapi.store.access.Qualifier[][])}
		 *
		 *
		 * @param startKeyValue the start of the scan, or {@code null} if a full table scan is desired
		 * @param startSearchOperator the operator for the start. Can be any of
		 * {@link org.apache.derby.iapi.store.access.ScanController#GT}, {@link org.apache.derby.iapi.store.access.ScanController#GE}, {@link org.apache.derby.iapi.store.access.ScanController#NA}
		 * @param stopKeyValue the stop of the scan, or {@code null} if a full table scan is desired.
		 * @param stopSearchOperator the operator for the stop. Can be any of
		 * {@link org.apache.derby.iapi.store.access.ScanController#GT}, {@link org.apache.derby.iapi.store.access.ScanController#GE}, {@link org.apache.derby.iapi.store.access.ScanController#NA}
		 * @param qualifiers scan qualifiers to use. This is used to construct equality filters to reduce
		 *                   the amount of data returned.
		 * @param sortOrder a sort order to use in how data is to be searched, or {@code null} if the default sort is used.
		 * @param scanColumnList a bitset determining which columns should be returned by the scan.
		 * @param transactionId the transactionId to use
		 * @return a transactionally aware scan from {@code startKeyValue} to {@code stopKeyValue}, with appropriate
		 * filters aas specified by {@code qualifiers}
		 * @throws IOException if {@code startKeyValue}, {@code stopKeyValue}, or {@code qualifiers} is unable to be
		 * properly serialized into a byte[].
		 */
		public static Scan setupScan(DataValueDescriptor[] startKeyValue, int startSearchOperator,
																 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
																 Qualifier[][] qualifiers,
																 boolean[] sortOrder,
																 FormatableBitSet scanColumnList,
																 String transactionId,boolean sameStartStopPosition, 
																 int[] formatIds, int[] columnOrdering, DataValueFactory dataValueFactory) throws StandardException {
				Scan scan = SpliceUtils.createScan(transactionId, scanColumnList!=null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
				scan.setCaching(DEFAULT_CACHE_SIZE);
				try{
						attachScanKeys(scan, startKeyValue, startSearchOperator,
										stopKeyValue, stopSearchOperator,
										qualifiers, scanColumnList, sortOrder,sameStartStopPosition, formatIds, columnOrdering, dataValueFactory);

						buildPredicateFilter(startKeyValue, startSearchOperator, qualifiers, scanColumnList, scan);
				}catch(IOException e){
						throw Exceptions.parseException(e);
				}
				return scan;
		}

		public static void buildPredicateFilter(DataValueDescriptor[] startKeyValue,
																						int startSearchOperator,
																						Qualifier[][] qualifiers,
																						FormatableBitSet scanColumnList, Scan scan) throws StandardException, IOException {
				EntryPredicateFilter pqf = getPredicates(startKeyValue,startSearchOperator,qualifiers,scanColumnList);
				scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,pqf.toBytes());
		}

		public static EntryPredicateFilter getPredicates(DataValueDescriptor[] startKeyValue,
																										 int startSearchOperator,
																										 Qualifier[][] qualifiers,
																										 FormatableBitSet scanColumnList) throws StandardException {
				ObjectArrayList<Predicate> predicates;
				BitSet colsToReturn = new BitSet();
				if(qualifiers!=null){
						predicates = getQualifierPredicates(qualifiers);
						for(Qualifier[] qualifierList:qualifiers){
								for(Qualifier qualifier:qualifierList){
										colsToReturn.set(qualifier.getColumnId()); //make sure that that column is returned
								}
						}
				}else
						predicates = ObjectArrayList.newInstanceWithCapacity(1);

				if(scanColumnList!=null){
						for(int i=scanColumnList.anySetBit();i>=0;i=scanColumnList.anySetBit(i)){
								colsToReturn.set(i);
						}
				}else{
						colsToReturn.clear(); //we want everything
				}
//        if(startKeyValue!=null && startSearchOperator != ScanController.GT){
//            Predicate indexPredicate = generateIndexPredicate(startKeyValue,startSearchOperator);
//            if(indexPredicate!=null)
//                predicates.add(indexPredicate);
//        }
				return new EntryPredicateFilter(colsToReturn,predicates,true);
		}

		public static ObjectArrayList<Predicate> getQualifierPredicates(Qualifier[][] qualifiers) throws StandardException {
        /*
         * Qualifiers are set up as follows:
         *
         * 1. qualifiers[0] is a list of AND clauses which MUST be satisfied
         * 2. [qualifiers[1]:] is a collection of OR clauses, of which at least one from each list must
         * be satisfied. E.g. for an i in [1:qualifiers.length], qualifiers[i] is a collection of OR clauses,
         * but ALL the OR-clause collections are bound together using an AND clause.
         */
				ObjectArrayList<Predicate> andPreds = ObjectArrayList.newInstanceWithCapacity(qualifiers[0].length);
				for(Qualifier andQual:qualifiers[0]){
						andPreds.add(getPredicate(andQual));
				}


				ObjectArrayList<Predicate> andedOrPreds = new ObjectArrayList<Predicate>();
				for(int i=1;i<qualifiers.length;i++){
						Qualifier[] orQuals = qualifiers[i];
						ObjectArrayList<Predicate> orPreds = ObjectArrayList.newInstanceWithCapacity(orQuals.length);
						for(Qualifier orQual:orQuals){
								orPreds.add(getPredicate(orQual));
						}
						andedOrPreds.add(new OrPredicate(orPreds));
				}
				if(andedOrPreds.size()>0)
						andPreds.addAll(andedOrPreds);

				Predicate firstAndPredicate = new AndPredicate(andPreds);
				return ObjectArrayList.from(firstAndPredicate);
		}

		private static Predicate getPredicate(Qualifier qualifier) throws StandardException {
				DataValueDescriptor dvd = qualifier.getOrderable();
				if(dvd==null||dvd.isNull()||dvd.isNullOp().getBoolean()){
						boolean filterIfMissing = qualifier.negateCompareResult();
						boolean isNullvalue = dvd==null||dvd.isNull();
						boolean isOrderedNulls = qualifier.getOrderedNulls();
						boolean isNullNumericalComparison = (isNullvalue && !isOrderedNulls);
						if(dvd==null)
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,false);
						else if(DerbyBytesUtil.isDoubleType(dvd))
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),true,false);
						else if(DerbyBytesUtil.isFloatType(dvd))
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,true);
						else
								return new NullPredicate(filterIfMissing,isNullNumericalComparison,qualifier.getColumnId(),false,false);
				}else{
						byte[] bytes = DerbyBytesUtil.generateBytes(dvd);
						return new ValuePredicate(getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
										qualifier.getColumnId(),
										bytes,true);
				}
		}

		public static Predicate generateIndexPredicate(DataValueDescriptor[] descriptors, int compareOp) throws StandardException {
				ObjectArrayList<Predicate> predicates = ObjectArrayList.newInstanceWithCapacity(descriptors.length);
				for(int i=0;i<descriptors.length;i++){
						DataValueDescriptor dvd = descriptors[i];
						if(dvd!=null &&!dvd.isNull() &&
										(dvd.getTypeFormatId() == StoredFormatIds.SQL_VARCHAR_ID || dvd.getTypeFormatId() == StoredFormatIds.SQL_CHAR_ID)){
								byte[] bytes = DerbyBytesUtil.generateBytes(dvd);
								if(bytes.length>0){
										predicates.add(new ValuePredicate(getCompareOp(compareOp,false),i,bytes,true));
								}
						}
				}
				return new AndPredicate(predicates);
		}

		/**
		 * Builds a key filter based on {@code startKeyValue}, {@code startSearchOperator}, and {@code qualifiers}.
		 *
		 * There are two separate filters that need to be constructed:
		 *
		 * 1. A <em>qualifier filter</em>, which filters out rows which don't match the specified qualifier criteria.
		 * 2. A <em>start-key filter</em>, which filters out rows which don't match the specified startKey.
		 *
		 * Situation 1 is straightforward--this method simply attaches a filter to ensure that only rows which
		 * have a column specified by {@qualifier} are returned.
		 *
		 * Situation2 is more complex. There are three basic states to be dealt with(best expressed in SQL):
		 *
		 * 1. "select * from t where t.key > start" or "select * from t where t.key >=start"
		 * 	(e.g. {@code startSearchOperator = }{@link ScanController#GT} or
		 * 2. "select * from t where t.key = start" (e.g. {@code startSearchOperator = }{@link ScanController#NA}
		 * 3. "select * from t where t.key < end" or "select * from t where t.key <=end"
		 *
		 * Case 1 and 3 are determined entirely by the start and end keys of the scan, so
		 * this method only attaches a filter in Case 2.
		 * @param startKeyValue the start key of the scan
		 * @param startSearchOperator the operator to check for whether or not to add a startkey scan
		 * @param qualifiers the qualifiers to filter on.
		 * @return a Filter which will filter out rows not matching {@code qualifiers} and which will
		 * satisfy the three interval cases.
		 * @throws IOException if {@code startKeyValue} or {@code qualifiers} cannot be serialized into a byte[].
		 */
		public static Filter buildKeyFilter(DataValueDescriptor[] startKeyValue,int startSearchOperator,
																				Qualifier[][] qualifiers) throws IOException {
				FilterList masterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				if(qualifiers!=null){
						masterList.addFilter(constructQualifierFilter(qualifiers));
				}

				if(startSearchOperator==ScanController.GT){
						return masterList;
				}

				//if startKeyValue == null, then we can't construct a filter anyway
				//so just  take whatever HBase gives us
				if(startKeyValue==null){
						return masterList;
				}
				masterList.addFilter(generateIndexFilter(startKeyValue, startSearchOperator));
				return masterList;
		}

		/* *************************************************************************************************/
	/*private helper methods*/
		private static Filter generateIndexFilter(DataValueDescriptor[] descriptors,int compareOp) throws IOException {
				FilterList masterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				try {
						for(int pos=0;pos<descriptors.length;pos++){
								DataValueDescriptor dvd = descriptors[pos];
								if(dvd!=null&&!dvd.isNull()){
					/*
					 * We have to check for whether or not the serialized bytes for this descriptor
					 * is actually populated--otherwise we would filter out all non-null entries.
					 */
										byte[] bytes = DerbyBytesUtil.generateBytes(dvd);
										if(bytes.length>0){
												masterList.addFilter(new SingleColumnValueFilter(SpliceConstants.DEFAULT_FAMILY_BYTES,
																Encoding.encode(pos),
																getCompareOp(compareOp,false),
																bytes));
										}
								}
						}
				} catch (StandardException e) {
						throw new IOException(e);
				}
				return masterList;
		}

		private static Filter constructQualifierFilter(Qualifier[][] qualifiers) throws IOException {
		/*
		 * Qualifiers are set up as follows:
		 *
		 * 1.qualifiers[0] is a list of AND clauses which MUST be satisfied
		 * 2.[qualifiers[1]:] is a collection of OR clauses, of which at least one from each must be
		 * satisfied. E.g. for an i in [1:qualifiers.length], qualifiers[i] is a collection of OR clauses,
		 * but ALL the OR-clause collections are bound together using an AND clause.
		 */
				FilterList finalFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				//build and clauses
				finalFilter.addFilter(buildFilter(qualifiers[0], FilterList.Operator.MUST_PASS_ALL));

				//build or clauses
				FilterList orList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				for(int clause=1;clause<qualifiers.length;clause++){
						orList.addFilter(buildFilter(qualifiers[clause], FilterList.Operator.MUST_PASS_ONE));
				}
				finalFilter.addFilter(orList);
				return finalFilter;
		}

		private static Filter buildFilter(Qualifier[] qualifiers, FilterList.Operator filterOp) throws IOException {
				FilterList list = new FilterList(filterOp);
				try {
						for(Qualifier qualifier: qualifiers){
								DataValueDescriptor dvd = qualifier.getOrderable();
								if(dvd==null||dvd.isNull()||dvd.isNullOp().getBoolean()){
										CompareFilter.CompareOp compareOp = qualifier.negateCompareResult()?
														CompareFilter.CompareOp.NOT_EQUAL: CompareFilter.CompareOp.EQUAL;

										boolean filterIfMissing = qualifier.negateCompareResult();

										boolean isNullValue = qualifier.getOrderable().isNull();  // is value null
										// isOrderedNulls == true => treat SQL null == null and less than or greater than all other values (ordered)
										boolean isOrderedNulls = qualifier.getOrderedNulls();
										// if value is null and nulls or unordered, we can't make a numerical comparison
										// between this null and other columns
										boolean isNullNumericalComparison = (isNullValue && ! isOrderedNulls);

										list.addFilter(new ColumnNullableFilter(SpliceConstants.DEFAULT_FAMILY_BYTES,
														Encoding.encode(qualifier.getColumnId()),
														isNullNumericalComparison, filterIfMissing));
								}else{
										byte[] bytes = DerbyBytesUtil.generateBytes(dvd);
										SingleColumnValueFilter filter = new SingleColumnValueFilter(SpliceConstants.DEFAULT_FAMILY_BYTES,
														Encoding.encode(qualifier.getColumnId()),
														getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
														bytes);
										filter.setFilterIfMissing(true);
										list.addFilter(filter);
								}
						}
				} catch (StandardException e) {
						throw new IOException(e);
				}
				return list;
		}

		private static CompareFilter.CompareOp getHBaseCompareOp(int operator, boolean negateResult) {
				if(negateResult){
						switch(operator){
								case DataValueDescriptor.ORDER_OP_EQUALS:
										return CompareFilter.CompareOp.NOT_EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSTHAN:
										return CompareFilter.CompareOp.GREATER_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_GREATERTHAN:
										return CompareFilter.CompareOp.LESS_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
										return CompareFilter.CompareOp.GREATER;
								case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
										return CompareFilter.CompareOp.LESS;
								default:
										throw new AssertionError("Unknown Derby operator "+ operator);
						}
				}else{
						switch(operator){
								case DataValueDescriptor.ORDER_OP_EQUALS:
										return CompareFilter.CompareOp.EQUAL;
								case DataValueDescriptor.ORDER_OP_LESSTHAN:
										return CompareFilter.CompareOp.LESS;
								case DataValueDescriptor.ORDER_OP_GREATERTHAN:
										return CompareFilter.CompareOp.GREATER;
								case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
										return CompareFilter.CompareOp.LESS_OR_EQUAL;
								case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
										return CompareFilter.CompareOp.GREATER_OR_EQUAL;
								default:
										throw new AssertionError("Unknown Derby operator "+ operator);
						}
				}
		}

		private static CompareFilter.CompareOp getCompareOp(int operator, boolean negate){
				if(negate){
						switch (operator){
								case ScanController.GT:
										return CompareFilter.CompareOp.LESS_OR_EQUAL;
								case ScanController.GE:
										return CompareFilter.CompareOp.LESS;
								default:
										return CompareFilter.CompareOp.EQUAL;
						}
				}else{
						switch(operator){
								case ScanController.GT:
										return CompareFilter.CompareOp.GREATER;
								case ScanController.GE:
										return CompareFilter.CompareOp.GREATER_OR_EQUAL;
								default:
										return CompareFilter.CompareOp.EQUAL;
						}
				}
		}


		private static void attachScanKeys(Scan scan, DataValueDescriptor[] startKeyValue, int startSearchOperator,
																			 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
																			 Qualifier[][] qualifiers,
																			 FormatableBitSet scanColumnList,
																			 boolean[] sortOrder,boolean sameStartStopPosition, 
																			 int[] formatIds, int[] columnOrdering, DataValueFactory dataValueFactory) throws IOException {
				if(scanColumnList!=null && (scanColumnList.anySetBit() != -1)) {
						scan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
				}
				try{						
						
						// Determines whether we can generate a key and also handles type conversion...
					
						boolean generateKey = true;
						if(startKeyValue!=null && stopKeyValue!=null){
							for (int i=0;i<startKeyValue.length;i++) {
								DataValueDescriptor startDesc = startKeyValue[i];
								if(startDesc ==null || startDesc.isNull()){
									generateKey=false;
									break;
								}
								if (columnOrdering != null && columnOrdering.length > 0 && 
										startDesc.getTypeFormatId() != formatIds[columnOrdering[i]]) {
									startKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(startDesc,formatIds[columnOrdering[i]],dataValueFactory);
								}
							}
							
							for (int i=0;i<stopKeyValue.length;i++) {
								DataValueDescriptor stopDesc = stopKeyValue[i];
								if (columnOrdering != null && columnOrdering.length > 0 && 
										stopDesc.getTypeFormatId() != formatIds[columnOrdering[i]]) {
									stopKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(stopDesc,formatIds[columnOrdering[i]],dataValueFactory);
								}
							}
							
						}

						if(generateKey){
								byte[] startRow = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue, startSearchOperator, sortOrder);
								scan.setStartRow(startRow);
								byte[] stopRow = DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue, stopSearchOperator, sortOrder);
								scan.setStopRow(stopRow);
								if(startRow==null)
										scan.setStartRow(HConstants.EMPTY_START_ROW);
								if(stopRow==null)
										scan.setStopRow(HConstants.EMPTY_END_ROW);
						}
				} catch (StandardException e) {
						throw new IOException(e);
				}
		}

		public static Pair<byte[],byte[]> getStartAndStopKeys(DataValueDescriptor[] startKeyValue, int startSearchOperator,
																													DataValueDescriptor[]stopKeyValue, int stopSearchOperator,
																													boolean[] sortOrder) throws IOException {
				try{
						boolean generateKey = true;
						if(startKeyValue!=null && stopKeyValue!=null){
								for(DataValueDescriptor startDesc: startKeyValue){
										if(startDesc ==null || startDesc.isNull()){
												generateKey=false;
												break;
										}
								}
						}

						//TODO -sf- do type-casting, overflow checks, etc. to make sure that we don't miss scans
						//TODO -sf- remove trailing null entries with Strings here
						if(generateKey){
								byte[] start = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue,startSearchOperator,sortOrder);
								byte[] stop  = DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue,stopSearchOperator,sortOrder);
								if(start==null)
										start = HConstants.EMPTY_START_ROW;
								if(stop==null)
										stop = HConstants.EMPTY_END_ROW;
								return Pair.newPair(start,stop);
						}else
								return Pair.newPair(null,null);
				} catch (StandardException e) {
						throw new IOException(e);
				}
		}
}
