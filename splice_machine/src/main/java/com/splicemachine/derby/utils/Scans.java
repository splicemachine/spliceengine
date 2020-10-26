/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.QualifierUtils;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.splicemachine.EngineDriver.isMemPlatform;
import static com.splicemachine.db.iapi.store.access.ScanController.GT;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;
import static com.splicemachine.db.shared.common.reference.SQLState.PARAMETER_CANNOT_BE_NULL;

/**
 * Utility methods and classes related to building HBase Scans
 *
 * @author jessiezhang
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
                                     ExecRow scannedRow,
                                     int[] keyDecodingMap,
                                     int[] keyTablePositionMap,
                                     DataValueFactory dataValueFactory,
                                     String tableVersion,
                                     boolean rowIdKey,
                                     SpliceConglomerate conglomerate,
                                     List<Pair<ExecRow, ExecRow>> keyRows,
                                     List<ExecRow> firstIndexColumnKeys) throws StandardException {
        assert dataValueFactory != null;
        DataScan scan =SIDriver.driver().getOperationFactory().newDataScan(txn);//SpliceUtils.createScan(txn, scanColumnList != null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
        scan.returnAllVersions();
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
                    sortOrder, scannedRow, keyTablePositionMap, keyDecodingMap,
                    dataValueFactory, tableVersion, rowIdKey,
                    conglomerate.getFormat_ids(),
                    keyRows, firstIndexColumnKeys);

            if (!rowIdKey) {
                buildPredicateFilter(qualifiers, scanColumnList, scan, keyDecodingMap);
            }


        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return scan;
    }


    public static Pair<byte[],byte[]> setupScanKey(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix, int stopSearchOperator,
                                 boolean[] sortOrder,
                                 int[] formatIds,
                                 int[] keyDecodingMap,
                                 int[] keyTablePositionMap,
                                 DataValueFactory dataValueFactory,
                                 String tableVersion,
                                 boolean rowIdKey,
                                 ExecRow templateRow,
                                 DataValueDescriptor scanKeyPrefix) throws StandardException {
        assert dataValueFactory != null;
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
            return getScanKey(startKeyValue, startSearchOperator,
                              stopKeyValue, stopKeyPrefix, stopSearchOperator,
                              sortOrder, formatIds, keyTablePositionMap, keyDecodingMap,
                              dataValueFactory, tableVersion, rowIdKey,
                              templateRow, scanKeyPrefix);

//            if (!rowIdKey) {
//                buildPredicateFilter(qualifiers, scanColumnList, scan, keyDecodingMap);
//            }


        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }


    public static DataScan setupScan(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                 DataValueDescriptor[] stopKeyValue, int stopSearchOperator,
                                 Qualifier[][] qualifiers,
                                 boolean[] sortOrder,
                                 FormatableBitSet scanColumnList,
                                 TxnView txn,
                                 boolean sameStartStopPosition,
                                 ExecRow scannedRow,
                                 int[] keyDecodingMap,
                                 int[] keyTablePositionMap,
                                 DataValueFactory dataValueFactory,
                                 String tableVersion,
                                 boolean rowIdKey,
                                 SpliceConglomerate conglomerate) throws StandardException {
        return setupScan(startKeyValue, startSearchOperator, stopKeyValue, null, stopSearchOperator, qualifiers,
                sortOrder, scanColumnList, txn, sameStartStopPosition, scannedRow, keyDecodingMap,
                keyTablePositionMap, dataValueFactory, tableVersion, rowIdKey, conglomerate, null, null);
    }

    public static void buildPredicateFilter(Qualifier[][] qualifiers,
                                            FormatableBitSet scanColumnList,
                                            int[] keyColumnEncodingMap,
                                            int[] columnTypes,
                                            DataScan scan,
                                            String tableVersion) throws StandardException, IOException {
        buildPredicateFilter(qualifiers, scanColumnList, scan, keyColumnEncodingMap);
    }

    public static void buildPredicateFilter(Qualifier[][] qualifiers,
                                            FormatableBitSet scanColumnList,
                                            DataScan scan,
                                            int[] keyColumnEncodingOrder) throws StandardException, IOException {
        EntryPredicateFilter pqf = getEntryPredicateFilter(qualifiers,
                scanColumnList, keyColumnEncodingOrder);
        scan.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL, pqf.toBytes());
    }

    public static EntryPredicateFilter getEntryPredicateFilter(Qualifier[][] qualifiers,
                                                     FormatableBitSet scanColumnList,
                                                     int[] keyColumnEncodingOrder) throws StandardException {
        BitSet colsToReturn = new BitSet();
        if (qualifiers != null) {
            for (Qualifier[] qualifierList : qualifiers) {
                for (Qualifier qualifier : qualifierList) {
                    colsToReturn.set(qualifier.getStoragePosition()); //make sure that that column is returned
                }
            }
        }
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
        return new EntryPredicateFilter(colsToReturn, true);
    }

    private static Pair<byte [], byte []>
    buildStartAndStopKeys(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                           DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix,
                           int stopSearchOperator,
                           boolean[] sortOrder,
                           ExecRow scannedRow, //template row
                           int[] keyTablePositionMap, //the location in the ENTIRE row of the key columns
                           int[] keyDecodingMap,
                           DataValueFactory dataValueFactory,
                           String tableVersion,
                           boolean rowIdKey) throws IOException {

        boolean generateStartKey = false;
        boolean generateStopKey = false;
        byte[] startRow = null;
        byte[] stopRow = null;

        try {
            // Determines whether we can generate a key and also handles type conversion...
            // gd according to Scott, startKey and stopKey are independent, so need to be evaluated as such


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
                        DataValueDescriptor targetDesc = scannedRow.getColumn(keyTablePositionMap[keyDecodingMap[i]] + 1); // the maps are 0-based, get Column is 1-based
                        if (!rowIdKey) {
                            startKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(startDesc, targetDesc, dataValueFactory, true);
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
                        DataValueDescriptor targetDesc = scannedRow.getColumn(keyTablePositionMap[keyDecodingMap[i]] + 1);
                        if (!rowIdKey) {
                            stop[i] = QualifierUtils.adjustDataValueDescriptor(stopDesc, targetDesc, dataValueFactory, false);
                        }
                    }
                }
            }

            if (generateStartKey) {
                startRow = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue, startSearchOperator, sortOrder, tableVersion, rowIdKey);

                if (startRow == null)
                    startRow = SIConstants.EMPTY_BYTE_ARRAY;
            }
            if (generateStopKey) {
                stopRow = DerbyBytesUtil.generateScanKeyForIndex(stop, stopSearchOperator, sortOrder, tableVersion, rowIdKey);
                if (stopKeyPrefix != null) {
                    stopRow = Bytes.unsignedCopyAndIncrement(stopRow);
                }

                if (stopRow == null)
                    stopRow = SIConstants.EMPTY_BYTE_ARRAY;
            }
        } catch (StandardException e) {
            throw new IOException(e);
        }
        return new Pair(startRow, stopRow);
    }

    private static void attachScanKeys(DataScan scan,
                                       DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                       DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix,
                                       int stopSearchOperator,
                                       boolean[] sortOrder,
                                       ExecRow scannedRow, //template row
                                       int[] keyTablePositionMap, //the location in the ENTIRE row of the key columns
                                       int[] keyDecodingMap,
                                       DataValueFactory dataValueFactory,
                                       String tableVersion,
                                       boolean rowIdKey,
                                       int[] columnTypes,
                                       List<Pair<ExecRow, ExecRow>> keyRows,
                                       List<ExecRow> firstIndexColumnKeys) throws IOException, StandardException {
        Pair<byte[], byte[]> startStopKeys;
        DataValueDescriptor[] startKeyDVDs = null, stopKeyDVDs = null;
        byte[] startKey;
        byte[] stopKey;

        if (firstIndexColumnKeys == null) {
            startStopKeys =
                buildStartAndStopKeys(startKeyValue, startSearchOperator,
                    stopKeyValue, stopKeyPrefix,
                    stopSearchOperator,
                    sortOrder,
                    scannedRow, //template row
                    keyTablePositionMap, //the location in the ENTIRE row of the key columns
                    keyDecodingMap,
                    dataValueFactory,
                    tableVersion,
                    rowIdKey);

            startKey = startStopKeys.getFirst();
            stopKey = startStopKeys.getSecond();

            if (startKey != null)
                scan.startKey(startKey);

            if (stopKey != null)
                scan.stopKey(stopKey);
        }
        else {
            if (keyRows != null)
                throw StandardException.newException(LANG_INTERNAL_ERROR,
                    "keyRows and startKeyValue not expected to both be set in attachScanKeys.");

            List<Pair<ExecRow, ExecRow>> newKeyRows = new ArrayList<>();
            ExecRow startRow, stopRow;
            final int startKeyLength = startKeyValue == null ? 1 : startKeyValue.length;
            final int stopKeyLength = stopKeyValue == null ? 1 : stopKeyValue.length;
            List<Pair<byte [], byte []>> keys = new ArrayList<>();
            for (ExecRow keyPrefixRow:firstIndexColumnKeys) {
                startRow = new ValueRow(startKeyLength);
                stopRow = new ValueRow(stopKeyLength);
                startRow.setColumn(1, keyPrefixRow.getColumn(1));
                stopRow.setColumn(1, keyPrefixRow.getColumn(1));

                startKeyDVDs = startKeyValue == null ? new DataValueDescriptor[1] : startKeyValue.clone();
                stopKeyDVDs = stopKeyValue == null ? new DataValueDescriptor[1] : stopKeyValue.clone();

                startKeyDVDs[0] = keyPrefixRow.getColumn(1);
                stopKeyDVDs[0]  = keyPrefixRow.getColumn(1);
                startStopKeys =
                  buildStartAndStopKeys(startKeyDVDs, ScanController.GE,
                                        stopKeyDVDs, null,
                                        GT,
                                        sortOrder,
                                        scannedRow, //template row
                                        keyTablePositionMap, //the location in the ENTIRE row of the key columns
                                        keyDecodingMap,
                                        dataValueFactory,
                                        tableVersion,
                                        false);
                startKey = startStopKeys.getFirst();
                stopKey = startStopKeys.getSecond();
                if (startKey != null && stopKey != null)
                    keys.add(new Pair(startKey, stopKey));
            }
            // Convert the list of keys into a MultiRowRangeFilter.
            if (!keys.isEmpty())
                scan.addRowkeyRangesFilter(keys);
        }

        if (keyRows != null && !isMemPlatform()) {
            List<Pair<byte [], byte []>> keys = new ArrayList<>();
            for (Pair<ExecRow, ExecRow> keyRow : keyRows) {
                if (keyRow.getFirst() != null)
                    startKeyDVDs = keyRow.getFirst().getClone().getRowArray();
                else
                    startKeyDVDs = null;

                if (keyRow.getSecond() != null)
                    stopKeyDVDs = keyRow.getSecond().getClone().getRowArray();
                else
                    stopKeyDVDs = null;
                startStopKeys =
                  buildStartAndStopKeys(startKeyDVDs, ScanController.GE,
                                        stopKeyDVDs, null,
                                        GT,
                                        sortOrder,
                                        scannedRow, //template row
                                        keyTablePositionMap, //the location in the ENTIRE row of the key columns
                                        keyDecodingMap,
                                        dataValueFactory,
                                        tableVersion,
                                        false);
                startKey = startStopKeys.getFirst();
                stopKey = startStopKeys.getSecond();
                if (startKey != null && stopKey != null)
                    keys.add(new Pair(startKey, stopKey));
            }

            // Convert the list of keys into a MultiRowRangeFilter.
            if (!keys.isEmpty())
                scan.addRowkeyRangesFilter(keys);
        }

    }

    private static Pair<byte[],byte[]> getScanKey(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                                  DataValueDescriptor[] stopKeyValue, DataValueDescriptor[] stopKeyPrefix,
                                                  int stopSearchOperator,
                                                  boolean[] sortOrder,
                                                  int[] columnTypes, //the types of the column in the ENTIRE Row
                                                  int[] keyTablePositionMap, //the location in the ENTIRE row of the key columns
                                                  int[] keyDecodingMap,
                                                  DataValueFactory dataValueFactory,
                                                  String tableVersion,
                                                  boolean rowIdKey,
                                                  ExecRow templateRow,
                                                  DataValueDescriptor scanKeyPrefix) throws IOException, StandardException {
        byte[] startRow;
        byte[] stopRow;
        try {
            boolean generateStartKey = false;
            boolean generateStopKey = false;

            if (startKeyValue != null) {
                generateStartKey = true;
                if (scanKeyPrefix != null)
                    startKeyValue[0] = scanKeyPrefix;
                for (int i = 0; i < startKeyValue.length; i++) {
                    DataValueDescriptor startDesc;
                    startDesc = startKeyValue[i];
                    if (startDesc == null) {
                        generateStartKey = false; // if any null encountered, don't make a start key
                        break;
                    }

                    // we just rely on key table positions
                    if (!isEmpty(keyDecodingMap) && keyDecodingMap[i] >= 0 && !isEmpty(keyTablePositionMap)) {
                        int targetColFormatId = columnTypes[keyTablePositionMap[keyDecodingMap[i]]];
                        DataValueDescriptor targetDesc = templateRow.getColumn(keyTablePositionMap[keyDecodingMap[i]] + 1); // the maps are 0-based, get Column is 1-based
                        if (!rowIdKey) {
                            startKeyValue[i] = QualifierUtils.adjustDataValueDescriptor(startDesc, targetDesc, dataValueFactory, true);
                        }
                    }
                }
            }
            DataValueDescriptor[] stop = stopKeyValue;
            if (stop == null)
                stop = stopKeyPrefix;
            if (stop == null && scanKeyPrefix != null) {
                // The value will be filled in in
                // the subsequent if statement block.
                stop = new DataValueDescriptor[1];
                // Make sure we don't include the stop key.
                stopSearchOperator = GT;
            }
            if (stop != null) {
                generateStopKey = true;
                if (scanKeyPrefix != null)
                    stop[0] = scanKeyPrefix;
                for (int i = 0; i < stop.length; i++) {
                    DataValueDescriptor stopDesc;
                    stopDesc = stop[i];
                    if (stopDesc == null) {
                        generateStopKey = false; // if any null encountered, don't make a stop key
                        break;
                    }

                    //  we just rely on key table positions
                    if (!isEmpty(keyDecodingMap) && !isEmpty(keyTablePositionMap)) {
                        int targetColFormatId = columnTypes[keyTablePositionMap[keyDecodingMap[i]]];
                        DataValueDescriptor targetDesc = templateRow.getColumn(keyTablePositionMap[keyDecodingMap[i]] + 1); // the maps are 0-based, get Column is 1-based
                        if (!rowIdKey) {
                            stop[i] = QualifierUtils.adjustDataValueDescriptor(stopDesc, targetDesc, dataValueFactory, false);
                        }
                    }
                }
            }

            if (generateStartKey) {
                startRow = DerbyBytesUtil.generateScanKeyForIndex(startKeyValue, startSearchOperator, sortOrder, tableVersion, rowIdKey);
                if (startRow == null)
                    throw StandardException.newException(PARAMETER_CANNOT_BE_NULL, "startRow");
            }
            else
                throw StandardException.newException(PARAMETER_CANNOT_BE_NULL, "startRow");

            if (generateStopKey) {
                stopRow = DerbyBytesUtil.generateScanKeyForIndex(stop, stopSearchOperator, sortOrder, tableVersion, rowIdKey);
                if (stopKeyPrefix != null) {
                    stopRow = Bytes.unsignedCopyAndIncrement(stopRow);
                }
                if (stopRow == null)
                    throw StandardException.newException(PARAMETER_CANNOT_BE_NULL, "stopRow");
            }
            else
                throw StandardException.newException(PARAMETER_CANNOT_BE_NULL, "stopRow");
        } catch (StandardException e) {
            throw new IOException(e);
        }
        return new Pair(startRow,stopRow);
    }

    private static boolean isEmpty(int[] array) {
        return array == null || array.length == 0;
    }


    /**
     * Process the qualifier list on the row, return true if it qualifies.
     * <p>
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is
     * optimized for the more frequent where no OR's are present.  The first
     * array slot is always a list of AND's to be treated as described above
     * for single dimensional AND qualifier arrays.  The subsequent slots are
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array
     * qual[][] argument is to be treated as the following, note if
     * qual.length = 1 then only the first array is valid and it is and an
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
     *
     * @return true if the row qualifies.
     *
     * @param row               The row being qualified.
     * @param qual_list         2 dimensional array representing conjunctive
     *                          normal form of simple qualifiers.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public static boolean qualifyRecordFromRow(
            Object[]        row,
            Qualifier[][]   qual_list,
            int[] baseColumnMap,
            DataValueDescriptor probeValue)
            throws StandardException {
        assert row!=null:"row passed in is null";
        assert qual_list!=null:"qualifier[][] passed in is null";
        boolean     row_qualifies = true;
        int numProbeValues = (probeValue != null && (probeValue instanceof ListDataType)) ?
            ((ListDataType) probeValue).getLength() : 1;
        for (int i = 0; i < qual_list[0].length; i++) {
            // process each AND clause
            row_qualifies = false;
            // process each OR clause.
            Qualifier q = qual_list[0][i];
            q.clearOrderableCache();
            // Get the column from the possibly partial row, of the
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue =
                    (DataValueDescriptor) row[baseColumnMap!=null?baseColumnMap[q.getStoragePosition()]:q.getStoragePosition()];
            if ( filterNull(q.getOperator(),columnValue,probeValue==null || i!=0?q.getOrderable():probeValue,q.getVariantType())) {
                return false;
            }

            row_qualifies =
                    columnValue.compare(
                            q.getOperator(),
                            q.getOrderable(),
                            q.getOrderedNulls(),
                            q.getUnknownRV());
            
            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;
//            System.out.println(String.format("And Clause -> value={%s}, operator={%s}, orderable={%s}, " +
//                    "orderedNulls={%s}, unknownRV={%s}",
//                    columnValue, q.getOperator(),q.getOrderable(),q.getOrderedNulls(),q.getUnknownRV()));
            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses
        for (int and_idx = 1; and_idx < qual_list.length; and_idx++) {
            // loop through each of the "and" clause.
            row_qualifies = false;
            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++) {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                q.clearOrderableCache();
                // Get the column from the possibly partial row, of the
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue =
                        (DataValueDescriptor) row[baseColumnMap!=null?baseColumnMap[q.getStoragePosition()]:q.getStoragePosition()];
                // do the compare between the column value and value in the
                // qualifier.
                if ( filterNull(q.getOperator(),columnValue,q.getOrderable(),q.getVariantType())) {
                    return false;
                }
                row_qualifies =
                        columnValue.compare(
                                q.getOperator(),
                                q.getOrderable(),
                                q.getOrderedNulls(),
                                q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;
                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd"
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }
        return(row_qualifies);
    }

    public static boolean filterNull(int operator, DataValueDescriptor columnValue, DataValueDescriptor orderable, int variantType) {
        if (orderable==null||orderable.isNull()) {
            switch (operator) {
                case DataType.ORDER_OP_LESSTHAN:
                case DataType.ORDER_OP_LESSOREQUALS:
                case DataType.ORDER_OP_GREATERTHAN:
                case DataType.ORDER_OP_GREATEROREQUALS:
                    return true;
                case DataType.ORDER_OP_EQUALS:
                    if (variantType != 1)
                        return true;
//                    if (columnValue == null || columnValue.isNull())
//                            return true;
                    return false;
            }
        }
        return false;
    }


}
