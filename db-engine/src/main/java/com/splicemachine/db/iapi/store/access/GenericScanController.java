/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;

/**
 * The set of interfaces implemented by all types of ScanControllers.
 * <p/>
 * A scan is the mechanism for iterating over the rows in a conglomerate,
 * the scan controller is the interface through which access clients
 * control the underlying scan.  An instance of a scan controller can
 * be thought of as an open scan.
 * <p/>
 * Scans are opened from a TransactionController.
 * <p/>
 * A ScanController can handle partial rows. Partial rows are described in
 * RowUtil.
 * <BR>
 * A scan controller is opened with a FormatableBitSet that describes the
 * columns that need to be returned on a fetch call. This FormatableBitSet
 * need not include any columns referenced in the qualifers, start
 * and/or stop keys.
 *
 * @see TransactionController#openScan
 * @see RowCountable
 * @see RowUtil
 */

public interface GenericScanController extends RowCountable,AutoCloseable{
    /**
     * Close the scan.  This method always succeeds, and never throws
     * any exceptions. Callers must not use the scan controller after
     * closing it; they are strongly advised to clear out the scan
     * controller reference after closing.
     *
     * @throws StandardException Standard exception policy.
     */
    @Override
    void close() throws StandardException;

    /**
     * Return ScanInfo object which describes performance of scan.
     * <p/>
     * Return ScanInfo object which contains information about the current
     * state of the scan.
     * <p/>
     * The statistics gathered by the scan are not reset to 0 by a reopenScan(),
     * rather they continue to accumulate.
     * <p/>
     *
     * @return The ScanInfo object which contains info about current scan.
     * @throws StandardException Standard exception policy.
     * @see ScanInfo
     */
    ScanInfo getScanInfo()
            throws StandardException;

    /**
     * Return whether this is a keyed conglomerate.
     * <p/>
     *
     * @return whether this is a keyed conglomerate.
     */
    boolean isKeyed();

    /**
     * Return whether this scan is table locked.
     * <p/>
     * Implementation of this is not complete.  Currently it does not give back
     * the right information on covering locks or lock escalation.  If the
     * openScan() caller specifies a MODE_TABLE as the lock_level then this
     * routine will always return true.  If the openScan() caller specifies a
     * MODE_RECORD as the lock_level then this routine will return true iff
     * the lock level of the system has been overridden either by the
     * db.storage.rowLocking=false property, or by a shipped
     * configuration which disables row locking.
     * <p/>
     *
     * @return whether this scan is table locked.
     */
    boolean isTableLocked();

    /**
     * Return a row location object to be used in calls to fetchLocation.
     * <p/>
     * Return a row location object of the correct type to be used in calls to
     * fetchLocation.
     * <p/>
     *
     * @return a row location object to be used in calls to fetchLocation.
     * @throws StandardException Standard exception policy.
     */
    RowLocation newRowLocationTemplate()
            throws StandardException;

    /**
     * Reposition the current scan.  This call is semantically the same as if
     * the current scan had been closed and a openScan() had been called instead.
     * The scan is reopened with against the same conglomerate, and the scan
     * is reopened with the same "scan column list", "hold" and "forUpdate"
     * parameters passed in the original openScan.
     * <p/>
     * The statistics gathered by the scan are not reset to 0 by a reopenScan(),
     * rather they continue to accumulate.
     *
     * @param startKeyValue       An indexable row which holds a
     *                            (partial) key value which, in combination with the
     *                            startSearchOperator, defines the starting position of
     *                            the scan.  If null, the starting position of the scan
     *                            is the first row of the conglomerate.
     * @param startSearchOperator an operator which defines
     *                            how the startKeyValue is to be searched for.  If
     *                            startSearchOperator is ScanController.GE, the scan starts on
     *                            the first row which is greater than or equal to the
     *                            startKeyValue.  If startSearchOperation is ScanController.GT,
     *                            the scan starts on the first row whose key is greater than
     *                            startKeyValue.  The startSearchOperation parameter is
     *                            ignored if the startKeyValue parameter is null.
     * @param qualifier           An array of qualifiers which, applied
     *                            to each key, restrict the rows returned by the scan.  Rows
     *                            for which any one of the qualifiers returns false are not
     *                            returned by the scan. If null, all rows are returned.
     * @param stopKeyValue        An indexable row which holds a
     *                            (partial) key value which, in combination with the
     *                            stopSearchOperator, defines the ending position of
     *                            the scan.  If null, the ending position of the scan
     *                            is the last row of the conglomerate.
     * @param stopSearchOperator  an operator which defines
     *                            how the stopKeyValue is used to determine the scan stopping
     *                            position. If stopSearchOperation is ScanController.GE, the scan
     *                            stops just before the first row which is greater than or
     *                            equal to the stopKeyValue.  If stopSearchOperation is
     *                            ScanController.GT, the scan stops just before the first row whose
     *                            key is greater than	startKeyValue.  The stopSearchOperation
     *                            parameter is ignored if the stopKeyValue parameter is null.
     * @throws StandardException Standard exception policy.
     */
    void reopenScan(
            DataValueDescriptor[] startKeyValue,
            int startSearchOperator,
            Qualifier qualifier[][],
            DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator)
            throws StandardException;

    /**
     * Reposition the current scan.  This call is semantically the same as if
     * the current scan had been closed and a openScan() had been called instead.
     * The scan is reopened against the same conglomerate, and the scan
     * is reopened with the same "scan column list", "hold" and "forUpdate"
     * parameters passed in the original openScan.
     * <p/>
     * The statistics gathered by the scan are not reset to 0 by a reopenScan(),
     * rather they continue to accumulate.
     * <p/>
     * Note that this operation is currently only supported on Heap conglomerates.
     * Also note that order of rows within are heap are not guaranteed, so for
     * instance positioning at a RowLocation in the "middle" of a heap, then
     * inserting more data, then continuing the scan is not guaranteed to see
     * the new rows - they may be put in the "beginning" of the heap.
     *
     * @param startRowLocation An existing RowLocation within the conglomerate,
     *                         at which to position the start of the scan.  The scan will begin at this
     *                         location and continue forward until the end of the conglomerate.
     *                         Positioning at a non-existent RowLocation (ie. an invalid one or one that
     *                         had been deleted), will result in an exception being thrown when the
     *                         first next operation is attempted.
     * @param qualifier        An array of qualifiers which, applied
     *                         to each key, restrict the rows returned by the scan.  Rows
     *                         for which any one of the qualifiers returns false are not
     *                         returned by the scan. If null, all rows are returned.
     * @throws StandardException Standard exception policy.
     */
    void reopenScanByRowLocation(
            RowLocation startRowLocation,
            Qualifier qualifier[][])
            throws StandardException;
}
