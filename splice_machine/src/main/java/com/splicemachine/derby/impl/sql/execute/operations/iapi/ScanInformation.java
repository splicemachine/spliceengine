/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;


import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.storage.DataScan;

import java.util.List;

/**
 * Represents metadata around Scanning operations. One implementation will delegate down to Derby,
 * another to some other method, depending on the shape of the implementation.
 *
 * @author Scott Fines
 * Created on: 10/1/13
 */
public interface ScanInformation<T> {

    void initialize(SpliceOperationContext opContext) throws StandardException;

    ExecRow getResultRow() throws StandardException;

    boolean isKeyed() throws StandardException;

    FormatableBitSet getAccessedColumns() throws StandardException;

    FormatableBitSet getAccessedNonPkColumns() throws StandardException;

		/**
		 * Get the key columns which are accessed WITH RESPECT to their position in the key itself.
		 *
		 * @return a bit set representing the key column locations which are interesting to the query.
		 * @throws StandardException
		 */
    FormatableBitSet getAccessedPkColumns() throws StandardException;

    int[] getIndexToBaseColumnMap() throws StandardException;

    DataScan getScan(TxnView txn) throws StandardException;

    DataScan getScan(TxnView txn, T startKeyHint,int[] keyDecodingMap, int[] scanKeys, T stopKeyPrefix) throws StandardException;

    Qualifier[][] getScanQualifiers() throws StandardException;

    long getConglomerateId();
    
    List<DataScan> getScans(TxnView txn, ExecRow startKeyOverride, Activation activation, int[] keyDecodingMap) throws StandardException;

    int[] getColumnOrdering() throws StandardException;

    SpliceConglomerate getConglomerate() throws StandardException;

    ExecIndexRow getStartPosition() throws StandardException;
}
