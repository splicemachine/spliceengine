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

package com.splicemachine.db.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;


/**
 * This abstract class describes compiled constants that are passed into
 * Delete, Insert, and Update ResultSets.
 *
 * This class and its sub-classes are not really implementations
 * of ConstantAction, since they are not executed.
 *
 * A better name for these classes would be 'Constants'.
 * E.g. WriteCursorConstants, DeleteConstants.
 *
 * Ideally one day the split will occur.
 */
public abstract class WriteCursorConstantAction implements ConstantAction, Formatable {

    /**
     * *****************************************************
     * *
     * *    This class implements Formatable. But it is NOT used
     * *    across either major or minor releases.  It is only
     * *     written persistently in stored prepared statements,
     * *    not in the replication stage.  SO, IT IS OK TO CHANGE
     * *    ITS read/writeExternal.
     * *
     * ******************************************************
     */

    long conglomId;
    StaticCompiledOpenConglomInfo heapSCOCI;
    IndexRowGenerator[] irgs;
    long[] indexCIDS;
    StaticCompiledOpenConglomInfo[] indexSCOCIs;
    String[] indexNames;
    boolean deferred;
    private Properties targetProperties;
    UUID targetUUID;
    int lockMode;
    private FKInfo[] fkInfo;
    private TriggerInfo triggerInfo;

    private ExecRow emptyHeapRow;
    private FormatableBitSet baseRowReadList;
    private int[] baseRowReadMap;
    private int[] streamStorableHeapColIds;
    boolean singleRowSource;
    protected int[] pkColumns;


    // CONSTRUCTORS

    /**
     * Public niladic constructor. Needed for Formatable interface to work.
     */
    public WriteCursorConstantAction() {
    }

    /**
     * Make the ConstantAction for a DELETE, INSERT, or UPDATE statement.
     *
     * @param conglomId                Conglomerate ID of heap.
     * @param heapSCOCI                StaticCompiledOpenConglomInfo for heap.
     * @param irgs                     Index descriptors
     * @param indexCIDS                Conglomerate IDs of indices
     * @param indexSCOCIs              StaticCompiledOpenConglomInfos for indexes.
     * @param indexNames               Names of indices on this table for error reporting.
     * @param deferred                 True means process as a deferred update
     * @param targetProperties         Properties on the target table
     * @param targetUUID               UUID of target table
     * @param lockMode                 The lock mode to use on the target table
     * @param fkInfo                   Structure containing foreign key info, if any (may be null)
     * @param triggerInfo              Structure containing trigger info, if any (may be null)
     * @param emptyHeapRow             an empty heap row
     * @param baseRowReadMap           BaseRowReadMap[heapColId]->ReadRowColumnId. (0 based)
     * @param streamStorableHeapColIds Null for non rep. (0 based)
     * @param singleRowSource          Whether or not source is a single row source
     */
    public WriteCursorConstantAction(
            long conglomId,
            StaticCompiledOpenConglomInfo heapSCOCI,
            int[] pkColumns,
            IndexRowGenerator[] irgs,
            long[] indexCIDS,
            StaticCompiledOpenConglomInfo[] indexSCOCIs,
            String[] indexNames,
            boolean deferred,
            Properties targetProperties,
            UUID targetUUID,
            int lockMode,
            FKInfo[] fkInfo,
            TriggerInfo triggerInfo,
            ExecRow emptyHeapRow,
            FormatableBitSet baseRowReadList,
            int[] baseRowReadMap,
            int[] streamStorableHeapColIds,
            boolean singleRowSource
    ) {
        this.conglomId = conglomId;
        this.heapSCOCI = heapSCOCI;
        this.pkColumns = pkColumns;
        this.irgs = irgs;
        this.indexSCOCIs = indexSCOCIs;
        this.indexCIDS = indexCIDS;
        this.indexSCOCIs = indexSCOCIs;
        this.deferred = deferred;
        this.targetProperties = targetProperties;
        this.targetUUID = targetUUID;
        this.lockMode = lockMode;
        this.emptyHeapRow = emptyHeapRow;
        this.fkInfo = fkInfo;
        this.triggerInfo = triggerInfo;
        this.baseRowReadList = baseRowReadList;
        this.baseRowReadMap = baseRowReadMap;
        this.streamStorableHeapColIds = streamStorableHeapColIds;
        this.singleRowSource = singleRowSource;
        this.indexNames = indexNames;
        if (SanityManager.DEBUG) {
            if (fkInfo != null) {
                SanityManager.ASSERT(fkInfo.length != 0, "fkinfo array has no elements, if there are no foreign keys, then pass in null");
            }
        }
    }

    ///////////////////////////////////////////////////////////////////
    //
    //    ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////

    /**
     * Gets the foreign key information for this constant action.
     * A full list of foreign keys was compiled into this constant
     * action.
     *
     * @return the list of foreign keys to enforce for this action
     */
    public final FKInfo[] getFKInfo() {
        return fkInfo;
    }

    public int[] getPkColumns() {
        return pkColumns;
    }

    /**
     * Basically, the same as getFKInfo but for triggers.
     *
     * @return the triggers that should be fired
     */
    public TriggerInfo getTriggerInfo() {
        return triggerInfo;
    }

    public UUID getTargetUUID() {
        return targetUUID;
    }

    ///////////////////////////////////////////////////////////////////
    //
    // INTERFACE METHODS
    //
    ///////////////////////////////////////////////////////////////////

    /**
     * NOP routine. The work is done in InsertResultSet.
     *
     * @see ConstantAction#executeConstantAction
     */
    @Override
    public final void executeConstantAction(Activation activation) throws StandardException {
    }

    // Formatable methods

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomId = in.readLong();
        heapSCOCI = (StaticCompiledOpenConglomInfo) in.readObject();
        irgs = new IndexRowGenerator[ArrayUtil.readArrayLength(in)];
        ArrayUtil.readArrayItems(in, irgs);

        indexCIDS = ArrayUtil.readLongArray(in);
        indexSCOCIs = new StaticCompiledOpenConglomInfo[ArrayUtil.readArrayLength(in)];
        ArrayUtil.readArrayItems(in, indexSCOCIs);

        deferred = in.readBoolean();
        targetProperties = (Properties) in.readObject();
        targetUUID = (UUID) in.readObject();
        lockMode = in.readInt();

        fkInfo = new FKInfo[ArrayUtil.readArrayLength(in)];
        ArrayUtil.readArrayItems(in, fkInfo);

        triggerInfo = (TriggerInfo) in.readObject();

        baseRowReadList = (FormatableBitSet) in.readObject();
        baseRowReadMap = ArrayUtil.readIntArray(in);
        streamStorableHeapColIds = ArrayUtil.readIntArray(in);
        singleRowSource = in.readBoolean();
        indexNames = ArrayUtil.readStringArray(in);

        if (in.readBoolean()) {
            pkColumns = new int[in.readInt()];
            for (int i = 0; i < pkColumns.length; i++) {
                pkColumns[i] = in.readInt();
            }
        }
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomId);
        out.writeObject(heapSCOCI);
        ArrayUtil.writeArray(out, irgs);
        ArrayUtil.writeLongArray(out, indexCIDS);
        ArrayUtil.writeArray(out, indexSCOCIs);
        out.writeBoolean(deferred);
        out.writeObject(targetProperties);
        out.writeObject(targetUUID);
        out.writeInt(lockMode);
        ArrayUtil.writeArray(out, fkInfo);

        //
        //Added for Xena.
        out.writeObject(triggerInfo);

        //
        //Moved from super class for Xena.
        out.writeObject(baseRowReadList);

        //
        //Added for Xena
        ArrayUtil.writeIntArray(out, baseRowReadMap);
        ArrayUtil.writeIntArray(out, streamStorableHeapColIds);

        //Added for Buffy
        out.writeBoolean(singleRowSource);

        // Added for Mulan (Track Bug# 3322)
        ArrayUtil.writeArray(out, indexNames);

        out.writeBoolean(pkColumns != null);
        if (pkColumns != null) {
            out.writeInt(pkColumns.length);
            for (int pkColumn : pkColumns) {
                out.writeInt(pkColumn);
            }
        }


    }

    // ACCESSORS

    /**
     * Get the conglomerate id for the changed heap.
     */
    public long getConglomerateId() {
        return conglomId;
    }

    /**
     * Get emptyHeapRow
     *
     * @param lcc The LanguageConnectionContext to use.
     * @return an empty base table row for the table being updated.
     */
    public ExecRow getEmptyHeapRow(LanguageConnectionContext lcc) throws StandardException {
        DataDictionary dd;
        TableDescriptor td;

        if (emptyHeapRow == null) {

            dd = lcc.getDataDictionary();

            td = dd.getTableDescriptor(targetUUID);

            emptyHeapRow = td.getEmptyExecRow();
        }

        return emptyHeapRow.getClone();
    }

    /**
     * Get the targetProperties from the constant action.
     */
    public Properties getTargetProperties() {
        return targetProperties;
    }

    /**
     * The the value of the specified key, if it exists, from
     * the targetProperties.
     *
     * @param key The key to search for
     * @return The value for the specified key if it exists, otherwise null.
     * (Return null if targetProperties is null.)
     */
    public String getProperty(String key) {
        return (targetProperties == null) ? null : targetProperties.getProperty(key);
    }

    public FormatableBitSet getBaseRowReadList() {
        return baseRowReadList;
    }

    public int[] getBaseRowReadMap() {
        return baseRowReadMap;
    }

    public int[] getStreamStorableHeapColIds() {
        return streamStorableHeapColIds;
    }

    /**
     * get the index name given the conglomerate id of the index.
     *
     * @param indexCID conglomerate ID of the index.
     * @return index name of given index.
     */
    public String getIndexNameFromCID(long indexCID) {
        int size = indexCIDS.length;

        if (indexNames == null) {
            return null;
        }

        for (int i = 0; i < size; i++) {
            if (indexCIDS[i] == indexCID)
                return indexNames[i];
        }
        return null;
    }

    public String[] getIndexNames() {
        return indexNames;
    }
}
 
