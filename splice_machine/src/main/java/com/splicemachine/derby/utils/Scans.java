/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.derby.impl.sql.execute.operations.QualifierUtils;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import org.apache.log4j.Logger;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;

/**
 * Utility methods and classes related to building HBase Scans
 *
 * @author jessiezhang
 * @author johnleach
 * @author Scott Fines
 *         Created: 1/24/13 10:50 AM
 */
public class Scans extends SpliceUtils {
    private static Logger LOG = Logger.getLogger(Scans.class);
    private Scans() {
    } //can't construct me


    /**
     * Builds a Scan from qualified starts and stops.
     *
     * This method does the following:
     *
     * 1. builds a basic scan and attaches transaction information to it.
     * 2. Constructs start and stop keys for the scan based on {@code startKeyValue} and {@code stopKeyValue},
     * according to the following rules:
     * A. if {@code startKeyValue ==null}, then set "" as the start of the scan
     * B. if {@code startKeyValue !=null}, then serialize the startKeyValue into a start key and set that.
     * C. if {@code stopKeyValue ==null}, then set "" as the end of the scan
     * D. if {@code stopKeyValue !=null}, then serialize the stopKeyValue into a stop key and set that.
     * 3. Construct startKeyFilters as necessary
     *
     * @param startKeyValue       the start of the scan, or {@code null} if a full table scan is desired
     * @param startSearchOperator the operator for the start. Can be any of
     *                            {@link com.splicemachine.db.iapi.store.access.ScanController#GT}, {@link com.splicemachine.db.iapi.store.access.ScanController#GE}, {@link com.splicemachine.db.iapi.store.access.ScanController#NA}
     * @param stopKeyValue        the stop of the scan, or {@code null} if a full table scan is desired.
     * @param stopSearchOperator  the operator for the stop. Can be any of
     *                            {@link com.splicemachine.db.iapi.store.access.ScanController#GT}, {@link com.splicemachine.db.iapi.store.access.ScanController#GE}, {@link com.splicemachine.db.iapi.store.access.ScanController#NA}
     * @param qualifiers          scan qualifiers to use. This is used to construct equality filters to reduce
     *                            the amount of data returned.
     * @param sortOrder           a sort order to use in how data is to be searched, or {@code null} if the default sort is used.
     * @param scanColumnList      a bitset determining which columns should be returned by the scan.
     * @param txn                 the transaction to use
     * @return a transactionally aware scan from {@code startKeyValue} to {@code stopKeyValue}, with appropriate
     * filters aas specified by {@code qualifiers}
     */
    public static DataScan setupScan(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix, int stopSearchOperator,
                                 Qualifier[][] qualifiers,
                                 boolean[] sortOrder,
                                 FormatableBitSet scanColumnList,
                                 TxnView txn,
                                 boolean sameStartStopPosition,
                                 int[] formatIds,
                                 int[] startScanKeys,
                                 int[] keyDecodingMap,
                                 int[] keyTablePositionMap,
                                 DataValueFactory dataValueFactory,
                                 String tableVersion,
                                 boolean rowIdKey) throws StandardException {
        assert dataValueFactory != null;
        DataScan scan =SIDriver.driver().getOperationFactory().newDataScan(txn);//SpliceUtils.createScan(txn, scanColumnList != null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
        scan.returnAllVersions();
//        scan.cacheRows()
//        scan.setCaching(DEFAULT_CACHE_SIZE);
        try {
            if (rowIdKey) {
                DataValueDescriptor[] dvd = null;
                if (startKeyValue != null && startKeyValue.length > 0) {
                    dvd = new DataValueDescriptor[1];
                    dvd[0] = new HBaseRowLocation(Bytes.fromHex(startKeyValue[0].getString()));
                    startKeyValue = dvd;
                }

                if (stopKeyValue != null && stopKeyValue.length > 0) {
                    dvd = new DataValueDescriptor[1];
                    dvd[0] = new HBaseRowLocation(Bytes.fromHex(stopKeyValue[0].getString()));
                    stopKeyValue = dvd;
                }
            }
            attachScanKeys(scan, startKeyValue, startSearchOperator,
                    stopKeyValue, stopKeyPrefix, stopSearchOperator,
                    sortOrder, formatIds, startScanKeys, keyTablePositionMap, keyDecodingMap, dataValueFactory, tableVersion, rowIdKey);

            if (!rowIdKey) {
                PredicateBuilder pb = new PredicateBuilder(keyDecodingMap, sortOrder, formatIds, tableVersion);
                buildPredicateFilter(qualifiers, scanColumnList, scan, pb, keyDecodingMap);
            }


        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return scan;
    }

    public static DataScan setupScan(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
                                 Qualifier[][] qualifiers,
                                 boolean[] sortOrder,
                                 FormatableBitSet scanColumnList,
                                 TxnView txn,
                                 boolean sameStartStopPosition,
                                 int[] formatIds,
                                 int[] keyDecodingMap,
                                 int[] keyTablePositionMap,
                                 DataValueFactory dataValueFactory,
                                 String tableVersion,
                                 boolean rowIdKey) throws StandardException {
        return setupScan(startKeyValue, startSearchOperator, stopKeyValue, null, stopSearchOperator, qualifiers,
                sortOrder, scanColumnList, txn, sameStartStopPosition, formatIds, null, keyDecodingMap,
                keyTablePositionMap, dataValueFactory, tableVersion, rowIdKey);
    }

    public static void buildPredicateFilter(Qualifier[][] qualifiers,
                                            FormatableBitSet scanColumnList,
                                            int[] keyColumnEncodingMap,
                                            int[] columnTypes,
                                            DataScan scan,
                                            String tableVersion) throws StandardException, IOException {
        PredicateBuilder pb = new PredicateBuilder(keyColumnEncodingMap, null, columnTypes, tableVersion);
        buildPredicateFilter(qualifiers, scanColumnList, scan, pb, keyColumnEncodingMap);
    }

    public static void buildPredicateFilter(Qualifier[][] qualifiers,
                                            FormatableBitSet scanColumnList,
                                            DataScan scan,
                                            PredicateBuilder pb,
                                            int[] keyColumnEncodingOrder) throws StandardException, IOException {
        EntryPredicateFilter pqf = getPredicates(qualifiers,
                scanColumnList, pb, keyColumnEncodingOrder);
        scan.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL, pqf.toBytes());
    }

    public static EntryPredicateFilter getPredicates(Qualifier[][] qualifiers,
                                                     FormatableBitSet scanColumnList,
                                                     PredicateBuilder pb,
                                                     int[] keyColumnEncodingOrder) throws StandardException {
        ObjectArrayList<Predicate> predicates;
        BitSet colsToReturn = new BitSet();
        if (qualifiers != null) {
            predicates = getQualifierPredicates(qualifiers, pb);
            for (Qualifier[] qualifierList : qualifiers) {
                for (Qualifier qualifier : qualifierList) {
                    colsToReturn.set(qualifier.getColumnId()); //make sure that that column is returned
                }
            }
        } else
            predicates = ObjectArrayList.newInstanceWithCapacity(1);

        if (scanColumnList != null) {
            for (int i = scanColumnList.anySetBit(); i >= 0; i = scanColumnList.anySetBit(i)) {
                colsToReturn.set(i);
            }
        } else {
            colsToReturn.clear(); //we want everything
        }

        //exclude any primary key columns
        if (keyColumnEncodingOrder != null && keyColumnEncodingOrder.length > 0) {
            for (int col : keyColumnEncodingOrder) {
                if (col >= 0)
                    colsToReturn.clear(col);
            }
        }
        return new EntryPredicateFilter(colsToReturn, predicates, true);
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
        for (Qualifier andQual : qualifiers[0]) {
            andPreds.add(pb.getPredicate(andQual));
        }


        ObjectArrayList<Predicate> andedOrPreds = new ObjectArrayList<Predicate>();
        for (int i = 1; i < qualifiers.length; i++) {
            Qualifier[] orQuals = qualifiers[i];
            ObjectArrayList<Predicate> orPreds = ObjectArrayList.newInstanceWithCapacity(orQuals.length);
            for (Qualifier orQual : orQuals) {
                orPreds.add(pb.getPredicate(orQual));
            }
            andedOrPreds.add(OrPredicate.or(orPreds));
        }
        if (andedOrPreds.size() > 0)
            andPreds.addAll(andedOrPreds);

        Predicate firstAndPredicate = AndPredicate.newAndPredicate(andPreds);
        return ObjectArrayList.from(firstAndPredicate);
    }

    private static void attachScanKeys(DataScan scan,
                                       DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                       DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix,
                                       int stopSearchOperator,
                                       boolean[] sortOrder,
                                       int[] columnTypes, //the types of the column in the ENTIRE Row
                                       int[] startScanKeys,
                                       int[] keyTablePositionMap, //the location in the ENTIRE row of the key columns
                                       int[] keyDecodingMap,
                                       DataValueFactory dataValueFactory,
                                       String tableVersion,
                                       boolean rowIdKey) throws IOException {
        try {
            // Determines whether we can generate a key and also handles type conversion...
            // gd according to Scott, startKey and stopKey are independent, so need to be evaluated as such
            boolean generateStartKey = false;
            boolean generateStopKey = false;

            if (startKeyValue != null) {
                generateStartKey = true;
                for (int i = 0; i < startKeyValue.length; i++) {
                    DataValueDescriptor startDesc = startKeyValue[i];
                    if (startDesc == null) {
                        generateStartKey = false; // if any null encountered, don't make a start key
                        break;
                    }

                    // we just rely on key table positions
                    if (!isEmpty(keyDecodingMap) && keyDecodingMap[i] >= 0 && !isEmpty(keyTablePositionMap)) {
                        int targetColFormatId = columnTypes[keyTablePositionMap[keyDecodingMap[i]]];
                        if (startDesc.getTypeFormatId() != targetColFormatId && !rowIdKey) {
                            startKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(startDesc, targetColFormatId, dataValueFactory);
                        }
                    }
                }
            }
            DataValueDescriptor[] stop = stopKeyValue;
            if (stop == null)
                stop = stopKeyPrefix;
            if (stop != null) {
                generateStopKey = true;
                for (int i = 0; i < stop.length; i++) {
                    DataValueDescriptor stopDesc = stop[i];
                    if (stopDesc == null) {
                        generateStopKey = false; // if any null encountered, don't make a stop key
                        break;
                    }

                    //  we just rely on key table positions
                    if (!isEmpty(keyDecodingMap) && !isEmpty(keyTablePositionMap)) {
                        int targetColFormatId = columnTypes[keyTablePositionMap[keyDecodingMap[i]]];
                        if (stopDesc.getTypeFormatId() != targetColFormatId && !rowIdKey) {
                            stop[i] = QualifierUtils.adjustDataValueDescriptor(stopDesc, targetColFormatId, dataValueFactory);
                        }
                    }
                }
            }

            if (generateStartKey) {
                byte[] startRow = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue, startSearchOperator, sortOrder, tableVersion, rowIdKey);
                scan.startKey(startRow);
                if (startRow == null)
                    scan.startKey(SIConstants.EMPTY_BYTE_ARRAY);
            }
            if (generateStopKey) {
                byte[] stopRow = DerbyBytesUtil.generateScanKeyForIndex(stop, stopSearchOperator, sortOrder, tableVersion, rowIdKey);
                if (stopKeyPrefix != null) {
                    stopRow = Bytes.unsignedCopyAndIncrement(stopRow);
                }
                scan.stopKey(stopRow);
                if (stopRow == null)
                    scan.stopKey(SIConstants.EMPTY_BYTE_ARRAY);
            }
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

    private static boolean isEmpty(int[] array) {
        if (array == null || array.length == 0) {
            return true;
        }
        return false;
    }
}
