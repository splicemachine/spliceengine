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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;


import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.RecordScan;
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

    RecordScan getScan(Txn txn) throws StandardException;

    RecordScan getScan(Txn txn, T startKeyHint, int[] keyDecodingMap, int[] scanKeys, T stopKeyPrefix) throws StandardException;

    Qualifier[][] getScanQualifiers() throws StandardException;

    long getConglomerateId();
    
    List<RecordScan> getScans(Txn txn, ExecRow startKeyOverride, Activation activation, int[] keyDecodingMap) throws StandardException;

    int[] getColumnOrdering() throws StandardException;

    SpliceConglomerate getConglomerate() throws StandardException;

    ExecIndexRow getStartPosition() throws StandardException;
}
