package com.splicemachine.derby.utils;

import java.io.IOException;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.storage.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.filter.ColumnNullableFilter;

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
			byte[] finish = BytesUtil.copyAndIncrement(start);
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

        byte[] finish = BytesUtil.copyAndIncrement(start);
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
     * @param startKeyValue the start of the scan, or {@code null} if a full table scan is desired
     * @param startSearchOperator the operator for the start. Can be any of
     * {@link ScanController#GT}, {@link ScanController#GE}, {@link ScanController#NA}
     * @param stopKeyValue the stop of the scan, or {@code null} if a full table scan is desired.
     * @param stopSearchOperator the operator for the stop. Can be any of
     * {@link ScanController#GT}, {@link ScanController#GE}, {@link ScanController#NA}
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
    public static Scan setupScan(DataValueDescriptor[] startKeyValue,int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
                                 Qualifier[][] qualifiers,
                                 boolean[] sortOrder,
                                 FormatableBitSet scanColumnList,
                                 String transactionId) throws StandardException {
        return setupScan(startKeyValue, startSearchOperator, stopKeyValue, stopSearchOperator, qualifiers, sortOrder,
                null, scanColumnList, transactionId);
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
     * @param startKeyValue the start of the scan, or {@code null} if a full table scan is desired
     * @param startSearchOperator the operator for the start. Can be any of
     * {@link ScanController#GT}, {@link ScanController#GE}, {@link ScanController#NA}
     * @param stopKeyValue the stop of the scan, or {@code null} if a full table scan is desired.
     * @param stopSearchOperator the operator for the stop. Can be any of
     * {@link ScanController#GT}, {@link ScanController#GE}, {@link ScanController#NA}
     * @param qualifiers scan qualifiers to use. This is used to construct equality filters to reduce
     *                   the amount of data returned.
     * @param sortOrder a sort order to use in how data is to be searched, or {@code null} if the default sort is used.
     * @param primaryKeys the primary keys to use in restricting the scan, or {@code null} if no primary key
     *                    restrictions are desired.
     * @param scanColumnList a bitset determining which columns should be returned by the scan.
     * @param transactionId the transactionId to use
     * @return a transactionally aware scan from {@code startKeyValue} to {@code stopKeyValue}, with appropriate
     * filters aas specified by {@code qualifiers}
     * @throws IOException if {@code startKeyValue}, {@code stopKeyValue}, or {@code qualifiers} is unable to be
     * properly serialized into a byte[].
     */
    public static Scan setupScan(DataValueDescriptor[] startKeyValue,int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
                                 Qualifier[][] qualifiers,
                                 boolean[] sortOrder,
                                 FormatableBitSet primaryKeys,
                                 FormatableBitSet scanColumnList,
                                 String transactionId) throws StandardException {
        Scan scan = SpliceUtils.createScan(transactionId, scanColumnList!=null);
        scan.setCaching(DEFAULT_CACHE_SIZE);
        try{
            attachScanKeys(scan, startKeyValue, startSearchOperator,
                    stopKeyValue, stopSearchOperator,
                    qualifiers, primaryKeys,scanColumnList, sortOrder);

            EntryPredicateFilter pqf = getPredicates(startKeyValue,startSearchOperator,qualifiers);
            ByteDataOutput bao  = new ByteDataOutput();
            bao.writeObject(pqf);
            scan.setAttribute("p",bao.toByteArray());
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
//		scan.setFilter(buildKeyFilter(startKeyValue, startSearchOperator, qualifiers));

		return scan;
	}

    private static EntryPredicateFilter getPredicates(DataValueDescriptor[] startKeyValue,
                                                      int startSearchOperator,
                                                      Qualifier[][] qualifiers) throws StandardException {
        List<Predicate> predicates;
        BitSet colsToReturn = new BitSet();
        if(qualifiers!=null){
            predicates = getQualifierPredicates(qualifiers);
            for(Qualifier[] qualifierList:qualifiers){
                for(Qualifier qualifier:qualifierList){
                    colsToReturn.set(qualifier.getColumnId()); //make sure that that column is returned
                }
            }
        }else
            predicates = Lists.newArrayListWithCapacity(1);

        Predicate indexPredicate = generateIndexPredicate(startKeyValue,startSearchOperator);
        if(indexPredicate!=null)
            predicates.add(indexPredicate);


        return new EntryPredicateFilter(colsToReturn,predicates);
    }

    private static List<Predicate> getQualifierPredicates(Qualifier[][] qualifiers) throws StandardException {
        /*
         * Qualifiers are set up as follows:
         *
         * 1. qualifiers[0] is a list of AND clauses which MUST be satisfied
         * 2. [qualifiers[1]:] is a collection of OR clauses, of which at least one from each list must
         * be satisfied. E.g. for an i in [1:qualifiers.length], qualifiers[i] is a collection of OR clauses,
         * but ALL the OR-clause collections are bound together using an AND clause.
         */
        List<Predicate> andPreds = Lists.newArrayListWithExpectedSize(qualifiers[0].length);
        for(Qualifier andQual:qualifiers[0]){
            andPreds.add(getPredicate(andQual));
        }

        Predicate firstAndPredicate = new AndPredicate(andPreds);

        List<Predicate> andedOrPreds = Lists.newArrayList();
        for(int i=1;i<qualifiers.length;i++){
            Qualifier[] orQuals = qualifiers[i];
            List<Predicate> orPreds = Lists.newArrayListWithCapacity(orQuals.length);
            for(Qualifier orQual:orQuals){
                orPreds.add(getPredicate(orQual));
            }
            andedOrPreds.add(new OrPredicate(orPreds));
        }
        Predicate secondAndPredicate = new AndPredicate(andedOrPreds);

        return Lists.newArrayList(firstAndPredicate,secondAndPredicate);
    }

    private static Predicate getPredicate(Qualifier qualifier) throws StandardException {
        DataValueDescriptor dvd = qualifier.getOrderable();
        if(dvd==null||dvd.isNull()||dvd.isNullOp().getBoolean()){
            boolean filterIfMissing = qualifier.negateCompareResult();
            boolean isNullvalue = dvd==null||dvd.isNull();
            boolean isOrderedNulls = qualifier.getOrderedNulls();
            boolean isNullNumericalComparison = (isNullvalue && !isOrderedNulls);
            //TODO -sf- this is not likely fully correct
            return new NullPredicate(filterIfMissing&&isNullNumericalComparison,qualifier.getColumnId());
        }else{
            byte[] bytes = DerbyBytesUtil.generateBytes(dvd);
            return new ValuePredicate(getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
                    qualifier.getColumnId(),
                                bytes,true);
        }
    }

    public static Predicate generateIndexPredicate(DataValueDescriptor[] descriptors, int compareOp) throws StandardException {
        List<Predicate> predicates = Lists.newArrayListWithCapacity(descriptors.length);
        for(int i=0;i<descriptors.length;i++){
            DataValueDescriptor dvd = descriptors[i];
            if(dvd!=null &&!dvd.isNull()){
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


	private static void attachScanKeys(Scan scan,DataValueDescriptor[] startKeyValue,int startSearchOperator,
																		DataValueDescriptor[] stopKeyValue,int stopSearchOperator,
																		Qualifier[][] qualifiers,
                                                                        FormatableBitSet primaryKeys,
                                                                        FormatableBitSet scanColumnList,
																		boolean[] sortOrder) throws IOException {
        scan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
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
				scan.setStartRow(DerbyBytesUtil.generateScanKeyForIndex(startKeyValue,startSearchOperator,sortOrder));
				scan.setStopRow(DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue,stopSearchOperator,sortOrder));

                /*
                 * It might happen that we'll want to do a primary key lookup (that is, we have primary keys
                 * that can be used to restrict the search). However, because Derby doesn't like treating Primary
                 * Keys specially, it's possible that no start or end value will be specified, but a qualifier
                 * will be used to indicate that a primary key is matched. This is because Derby thinks
                 * that it's doing a full table scan.
                 *
                 * However, we are smarter than Derby here, and can convert those qualifiers into start
                 * and stop keys based on how we know the primary key columns are structured.
                 *
                 * the algorithm goes something like this:
                 *
                 * Take the lowest order primary key, and get all the Qualifiers for that key. There are
                 * different states possible for the qualifiers:
                 *
                 * 1. pk < value
                 * 2. pk <= value
                 * 3. pk = value
                 * 4. pk >= value
                 * 5. pk > value
                 * 6. value1 <= pk < value2
                 * 7. value1 <= pk <= value2
                 * 8. value1 < pk <= value2
                 * 9. value1 < pk < value2
                 *
                 * If 1 or 2 is true, then we can set the qualifier into the stopKey, but we must terminate
                 * operating on the startKey, and the same argument in reverse works for case 4 and 5, while we
                 * can set both start and stop key values for cases 3, 6,7,8,9. We then repeat the process on
                 * all primary key columns, until either we run out of qualifiers or we can go no further with
                 * constructing the start and stop key.
                 *
                 * And then finally, we have a start and an end key, but perhaps what derby specified
                 * is smarter than this, and has a tighter bound, so in the end we need to compare what
                 * Derby generated with what we've got from our Qualifiers, and take the tighter bound.
                 */
                if(primaryKeys!=null && qualifiers!=null && qualifiers.length >0){

                    List<Qualifier> pkQualifiers = Lists.newArrayListWithCapacity(3);
                    List<byte[]> startKeyBounds = Lists.newArrayListWithCapacity(primaryKeys.getNumBitsSet());
                    List<byte[]> endKeyBounds = Lists.newArrayListWithCapacity(primaryKeys.getNumBitsSet());
                    boolean[] shouldContinue = new boolean[]{true,true};
                    boolean addStart=shouldContinue[0];
                    boolean addStop = shouldContinue[1];
                    for(int pkCol = primaryKeys.anySetBit();pkCol!=-1&&(addStart||addStop);pkCol = primaryKeys.anySetBit(pkCol)){
                        //empty out previous runs
                        pkQualifiers.clear();
                        for(Qualifier[] quals: qualifiers){
                            for(Qualifier qual:quals){
                                if(qual.getColumnId()==pkCol){
                                    pkQualifiers.add(qual);
                                }
                            }
                        }

                        if(pkQualifiers.size()<=0){
                            shouldContinue[0] = false;
                            shouldContinue[1] = false;
                            continue;
                        }
                        QualifierBounds qualifierBounds = QualifierBounds.getOperator(pkQualifiers);
                        qualifierBounds.process(pkQualifiers,startKeyBounds,endKeyBounds,shouldContinue);
                    }

                    byte[] qualifiedStart = BytesUtil.concat(startKeyBounds);
                    byte[] qualifiedStop = BytesUtil.concat(endKeyBounds);

                    if(qualifiedStart!=null&&qualifiedStart.length>0){
                        byte[] currentStart = scan.getStartRow();
                        if(currentStart==null) scan.setStartRow(qualifiedStart);
                        else if (Bytes.compareTo(currentStart,qualifiedStart) <0)
                            scan.setStartRow(qualifiedStart);
                    }
                    if(qualifiedStop!=null&&qualifiedStop.length>0){
                        byte[] currentStop = scan.getStopRow();
                        if(currentStop==null) scan.setStopRow(qualifiedStop);
                        else if (Bytes.compareTo(currentStop,qualifiedStop) <0)
                            scan.setStopRow(qualifiedStop);
                    }
                }
					/*
					 * If we can't fill the start and end rows correctly, we assume that the scan is empty.
					 * This causes problems later (particularly in shuffle tasks, where null start and end mean
					 * "go over the entire table" not "go over nothing") and in those cases, this will have to be
					 * undone.
					 */
                if(scan.getStartRow()==null)
					scan.setStartRow(HConstants.EMPTY_START_ROW);
				if(scan.getStopRow()==null)
					scan.setStopRow(HConstants.EMPTY_END_ROW);
			}
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}


}
