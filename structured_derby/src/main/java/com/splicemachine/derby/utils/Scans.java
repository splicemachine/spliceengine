package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.operations.QualifierUtils;
import com.splicemachine.storage.AndPredicate;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.OrPredicate;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Utility methods and classes related to building HBase Scans
 * @author jessiezhang
 * @author johnleach
 * @author Scott Fines
 * Created: 1/24/13 10:50 AM
 */
public class Scans extends SpliceUtils {

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
		 * 3. Construct startKeyFilters as necessary
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
		 */
		public static Scan setupScan(DataValueDescriptor[] startKeyValue, int startSearchOperator,
																 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
																 Qualifier[][] qualifiers,
																 boolean[] sortOrder,
																 FormatableBitSet scanColumnList,
																 String transactionId,
																 boolean sameStartStopPosition,
																 int[] formatIds,
																 int[] keyDecodingMap,
																 int[] keyTablePositionMap,
																 DataValueFactory dataValueFactory,
																 String tableVersion) throws StandardException {
				Scan scan = SpliceUtils.createScan(transactionId, scanColumnList!=null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
				scan.setCaching(DEFAULT_CACHE_SIZE);
				try{
						attachScanKeys(scan, startKeyValue, startSearchOperator,
										stopKeyValue, stopSearchOperator,
										scanColumnList, sortOrder, formatIds, keyTablePositionMap,dataValueFactory, tableVersion);


						PredicateBuilder pb = new PredicateBuilder(keyDecodingMap,sortOrder,formatIds,tableVersion);
						buildPredicateFilter(
										qualifiers, scanColumnList,scan,pb,keyDecodingMap);
				}catch(IOException e){
						throw Exceptions.parseException(e);
				}
				return scan;
		}

		public static void buildPredicateFilter(Qualifier[][] qualifiers,
																						FormatableBitSet scanColumnList,
																						int[] keyColumnEncodingMap,
																						int[] columnTypes,
																						Scan scan,
																						String tableVersion) throws StandardException, IOException {
				PredicateBuilder pb = new PredicateBuilder(keyColumnEncodingMap,null,columnTypes,tableVersion);
				buildPredicateFilter(qualifiers, scanColumnList, scan, pb, keyColumnEncodingMap);
		}

		public static void buildPredicateFilter(Qualifier[][] qualifiers,
																						FormatableBitSet scanColumnList,
																						Scan scan,
																						PredicateBuilder pb,
																						int[] keyColumnEncodingOrder) throws StandardException, IOException {
				EntryPredicateFilter pqf = getPredicates( qualifiers,
								scanColumnList, pb,keyColumnEncodingOrder);
				scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,pqf.toBytes());
		}

		public static EntryPredicateFilter getPredicates(Qualifier[][] qualifiers,
																										 FormatableBitSet scanColumnList,
																										 PredicateBuilder pb,
																										 int[] keyColumnEncodingOrder) throws StandardException {
				ObjectArrayList<Predicate> predicates;
				BitSet colsToReturn = new BitSet();
				if(qualifiers!=null){
						predicates = getQualifierPredicates(qualifiers,pb);
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

               //exclude any primary key columns
               if (keyColumnEncodingOrder != null && keyColumnEncodingOrder.length > 0) {
                   for (int col:keyColumnEncodingOrder) {
											 if(col>=0)
													 colsToReturn.clear(col);
                   }
               }
//        if(startKeyValue!=null && startSearchOperator != ScanController.GT){
//            Predicate indexPredicate = generateIndexPredicate(startKeyValue,startSearchOperator);
//            if(indexPredicate!=null)
//                predicates.add(indexPredicate);
//        }
				return new EntryPredicateFilter(colsToReturn,predicates,true);
		}

		public static ObjectArrayList<Predicate> getQualifierPredicates(Qualifier[][] qualifiers,
																																		PredicateBuilder pb) throws StandardException {
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
						andPreds.add(pb.getPredicate(andQual));
				}


				ObjectArrayList<Predicate> andedOrPreds = new ObjectArrayList<Predicate>();
				for(int i=1;i<qualifiers.length;i++){
						Qualifier[] orQuals = qualifiers[i];
						ObjectArrayList<Predicate> orPreds = ObjectArrayList.newInstanceWithCapacity(orQuals.length);
						for(Qualifier orQual:orQuals){
								orPreds.add(pb.getPredicate(orQual));
						}
						andedOrPreds.add(OrPredicate.or(orPreds));
				}
				if(andedOrPreds.size()>0)
						andPreds.addAll(andedOrPreds);

				Predicate firstAndPredicate = AndPredicate.newAndPredicate(andPreds);
				return ObjectArrayList.from(firstAndPredicate);
		}

		private static void attachScanKeys(Scan scan,
																			 DataValueDescriptor[] startKeyValue, int startSearchOperator,
																			 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
																			 FormatableBitSet scanColumnList,
																			 boolean[] sortOrder,
																			 int[] columnTypes, //the types of the column in the ENTIRE Row
																			 int[] keyTablePositionMap, //the location in the ENTIRE row of the key columns
																			 DataValueFactory dataValueFactory,
																			 String tableVersion) throws IOException {
				try {
						// Determines whether we can generate a key and also handles type conversion...

						boolean generateKey = true;
						if(startKeyValue!=null && stopKeyValue!=null){
								for (int i=0;i<startKeyValue.length;i++) {
										DataValueDescriptor startDesc = startKeyValue[i];
										if(startDesc ==null || startDesc.isNull()){
												generateKey=false;
												break;
										}
										if (keyTablePositionMap != null && keyTablePositionMap.length > 0 &&
														startDesc.getTypeFormatId() != columnTypes[keyTablePositionMap[i]]) {
												startKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(startDesc,columnTypes[keyTablePositionMap[i]],dataValueFactory);
										}
								}

								for (int i=0;i<stopKeyValue.length;i++) {
										DataValueDescriptor stopDesc = stopKeyValue[i];
										if (keyTablePositionMap != null && keyTablePositionMap.length > 0 &&
														stopDesc.getTypeFormatId() != columnTypes[keyTablePositionMap[i]]) {
												stopKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(stopDesc,columnTypes[keyTablePositionMap[i]],dataValueFactory);
										}
								}

						}

						if(generateKey){
								byte[] startRow = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue, startSearchOperator, sortOrder,tableVersion);
								scan.setStartRow(startRow);
								byte[] stopRow = DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue, stopSearchOperator, sortOrder,tableVersion);
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
}
