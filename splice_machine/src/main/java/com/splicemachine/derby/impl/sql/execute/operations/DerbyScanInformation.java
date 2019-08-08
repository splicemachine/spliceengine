/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SerializationUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.storage.DataScan;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class DerbyScanInformation implements ScanInformation<ExecRow>, Externalizable {
    private static final long serialVersionUID = 1l;
    //fields marked transient as a documentation tool, so we know which fields aren't set
    protected transient GenericStorablePreparedStatement gsps;
    protected transient Activation activation;
    public static final int[] Empty_Array = {};
    //serialized fields
    private String resultRowAllocatorMethodName;
    private String startKeyGetterMethodName;
    private String stopKeyGetterMethodName;
    private String scanQualifiersField;
    protected boolean sameStartStopPosition;
    private long conglomId;
    protected int startSearchOperator;
    protected int stopSearchOperator;
    protected boolean rowIdKey;

    //fields which are cached for performance
    private FormatableBitSet accessedCols;
    private FormatableBitSet accessedNonPkCols;
    private FormatableBitSet accessedPkCols;
    private SpliceMethod<ExecRow> resultRowAllocator;
    private SpliceMethod<ExecIndexRow> startKeyGetter;
    private SpliceMethod<ExecIndexRow> stopKeyGetter;
    private SpliceConglomerate conglomerate;
    private int colRefItem;
    private int indexColItem;
    private String tableVersion;
    private String defaultRowMethodName;
    private SpliceMethod<ExecRow> defaultRowAllocator;
    private int defaultValueMapItem;
    private FormatableBitSet defaultValueMap;

    @SuppressWarnings("UnusedDeclaration")
    @Deprecated
    public DerbyScanInformation() {
    }

    public DerbyScanInformation(String resultRowAllocatorMethodName,
                                String startKeyGetterMethodName,
                                String stopKeyGetterMethodName,
                                String scanQualifiersField,
                                long conglomId,
                                int colRefItem,
                                int indexColItem,
                                boolean sameStartStopPosition,
                                int startSearchOperator,
                                int stopSearchOperator,
                                boolean rowIdKey,
                                String tableVersion,
                                String defaultRowMethodName,
                                int defaultValueMapItem) {
        this.resultRowAllocatorMethodName = resultRowAllocatorMethodName;
        this.startKeyGetterMethodName = startKeyGetterMethodName;
        this.stopKeyGetterMethodName = stopKeyGetterMethodName;
        this.colRefItem = colRefItem;
        this.indexColItem = indexColItem;
        this.conglomId = conglomId;
        this.sameStartStopPosition = sameStartStopPosition;
        this.startSearchOperator = startSearchOperator;
        this.scanQualifiersField = scanQualifiersField;
        this.stopSearchOperator = stopSearchOperator;
        this.rowIdKey = rowIdKey;
        this.tableVersion = tableVersion;
        this.defaultRowMethodName = defaultRowMethodName;
        this.defaultValueMapItem = defaultValueMapItem;
    }

    @Override
    public void initialize(SpliceOperationContext opContext) throws StandardException {
        this.gsps = opContext.getPreparedStatement();
        this.activation = opContext.getActivation();
    }

    @Override
    public ExecRow getResultRow() throws StandardException {
        if (resultRowAllocator == null)
            resultRowAllocator = new SpliceMethod<>(resultRowAllocatorMethodName, activation);
        return resultRowAllocator.invoke();
    }

    @Override
    public ExecRow getDefaultRow() throws StandardException {
        if (defaultRowMethodName == null)
            return null;

        if (defaultRowAllocator == null)
            defaultRowAllocator = new SpliceMethod<>(defaultRowMethodName, activation);
        return defaultRowAllocator.invoke();
    }

    @Override
    public FormatableBitSet getDefaultValueMap() throws StandardException {
        if (defaultValueMap == null) {
            if (defaultValueMapItem == -1) {
                defaultValueMap = null;
            } else {
                defaultValueMap = (FormatableBitSet) gsps.getSavedObject(defaultValueMapItem);
            }
        }
        return defaultValueMap;
    }

    @Override
    public boolean isKeyed() throws StandardException {
        return getConglomerate().getTypeFormatId() == IndexConglomerate.FORMAT_NUMBER;
    }

    public SpliceConglomerate getConglomerate() throws StandardException {
        if (conglomerate == null)
            conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomId);
        return conglomerate;
    }

    @Override
    public FormatableBitSet getAccessedColumns() throws StandardException {
        if (accessedCols == null) {
            if (colRefItem == -1) {
                // accessed all columns
                accessedCols = null;
            } else {
                accessedCols = (FormatableBitSet) gsps.getSavedObject(colRefItem);
                accessedCols.grow(getConglomerate().getFormat_ids().length);
            }
        }
        return accessedCols;
    }


    @Override
    public FormatableBitSet getAccessedPkColumns() throws StandardException {
        if (accessedPkCols == null) {
            int[] keyColumnEncodingOrder = getColumnOrdering();
            if (keyColumnEncodingOrder == null) return null; //no keys to decode

            FormatableBitSet accessedColumns = getAccessedColumns();
            FormatableBitSet accessedKeyCols = new FormatableBitSet(keyColumnEncodingOrder.length);
            if (accessedColumns == null) {
                /*
                 * We need to access every column in the key
                 */
                for (int i = 0; i < keyColumnEncodingOrder.length; i++) {
                    accessedKeyCols.set(i);
                }
            } else {
                /*
                 * accessedColumns is the list of columns IN THE ENTIRE row
                 * which are being accessed. So if the row looks like (a,b,c,d) and
                 * I want (a,c) then accessColumns = {0,2}.
                 *
                 * I need to turn that into the columns which are present in the key,
                  * with reference to their position IN THE KEY(not in the entire row).
                 */
                for (int i = 0; i < keyColumnEncodingOrder.length; i++) {
                    int keyColumn = keyColumnEncodingOrder[i];
                    if (accessedColumns.get(keyColumn))
                        accessedKeyCols.set(i);
                }
            }
            accessedPkCols = accessedKeyCols;
        }
        return accessedPkCols;
    }

    @Override
    public FormatableBitSet getAccessedNonPkColumns() throws StandardException {
        if (accessedNonPkCols == null) {
            FormatableBitSet cols = getAccessedColumns();
            if (cols == null) {
                int size = getConglomerate().getFormat_ids().length;
                cols = new FormatableBitSet(size);
                for (int i = 0; i < size; ++i) {
                    cols.set(i);
                }
            }
            accessedNonPkCols = removePkCols(cols);
        }
        return accessedNonPkCols;
    }

    private FormatableBitSet removePkCols(FormatableBitSet cols) throws StandardException {

        int[] columnOrdering = getColumnOrdering();

        if (columnOrdering == null) {
            return cols;
        } else {
            FormatableBitSet result = new FormatableBitSet(cols);
            for (int col : columnOrdering) {
                result.clear(col);
            }
            return result;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(resultRowAllocatorMethodName);
        out.writeLong(conglomId);
        out.writeBoolean(sameStartStopPosition);
        out.writeInt(startSearchOperator);
        out.writeInt(stopSearchOperator);
        out.writeInt(colRefItem);
        out.writeInt(indexColItem);
        SerializationUtils.writeNullableString(scanQualifiersField, out);
        SerializationUtils.writeNullableString(startKeyGetterMethodName, out);
        SerializationUtils.writeNullableString(stopKeyGetterMethodName, out);
        out.writeUTF(tableVersion);
        out.writeBoolean(rowIdKey);
        SerializationUtils.writeNullableString(defaultRowMethodName, out);
        out.writeInt(defaultValueMapItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        resultRowAllocatorMethodName = in.readUTF();
        conglomId = in.readLong();
        sameStartStopPosition = in.readBoolean();
        startSearchOperator = in.readInt();
        stopSearchOperator = in.readInt();
        colRefItem = in.readInt();
        indexColItem = in.readInt();
        scanQualifiersField = SerializationUtils.readNullableString(in);
        startKeyGetterMethodName = SerializationUtils.readNullableString(in);
        stopKeyGetterMethodName = SerializationUtils.readNullableString(in);
        this.tableVersion = in.readUTF();
        this.rowIdKey = in.readBoolean();
        defaultRowMethodName = SerializationUtils.readNullableString(in);
        defaultValueMapItem = in.readInt();
    }

    @Override
    public DataScan getScan(TxnView txn) throws StandardException {
        return getScan(txn, null, null, null);
    }

    @Override
    public DataScan getScan(TxnView txn, ExecRow startKeyOverride, int[] keyDecodingMap, ExecRow stopKeyPrefix) throws StandardException {
        boolean sameStartStop = startKeyOverride == null && sameStartStopPosition;
        ExecRow startPosition = getStartPosition();
        ExecRow stopPosition = sameStartStop ? startPosition : getStopPosition();
        ExecRow overriddenStartPos = startKeyOverride != null ? startKeyOverride : startPosition;

        /*
         * if the stop position is the same as the start position, we are
         * right at the position where we should return values, and so we need to make sure that
         * we only return values which match an equals filter. Otherwise, we'll need
         * to scan between the start and stop keys and pull back the values which are greater than
         * or equals to the start (e.g. leave startSearchOperator alone).
         */
        if (sameStartStop) {
            startSearchOperator = ScanController.NA;
        }

        if (startKeyOverride != null) {
            startSearchOperator = ScanController.GE;
        }

        if (stopPosition == null && stopKeyPrefix!=null) {
            stopSearchOperator = ScanController.NA;
        }
        /* Below populateQualifiers can mutate underlying DataValueDescriptors in some cases, clone them for use in
           start/stop keys first. */
        DataValueDescriptor[] startKeyValues = overriddenStartPos == null ? null : overriddenStartPos.getClone().getRowArray();
        DataValueDescriptor[] stopKeyValues = stopPosition == null ? null : stopPosition.getClone().getRowArray();
        DataValueDescriptor[] stopPrefixValues = stopKeyPrefix == null ? null : stopKeyPrefix.getClone().getRowArray();
        Qualifier[][] qualifiers = populateQualifiers();

        getConglomerate();

        return Scans.setupScan(
                startKeyValues,
                startSearchOperator,
                stopKeyValues,
                stopPrefixValues,
                stopSearchOperator,
                qualifiers,
                conglomerate.getAscDescInfo(),
                getAccessedColumns(),
                txn, sameStartStop,
                conglomerate.getFormat_ids(),
                keyDecodingMap,
                FormatableBitSetUtils.toCompactedIntArray(getAccessedColumns()),
                activation.getDataValueFactory(),
                tableVersion,
                rowIdKey);
    }

    @Override
    public Qualifier[][] getScanQualifiers() throws StandardException {
        return populateQualifiers();
    }

    @Override
    public long getConglomerateId() {
        return conglomId;
    }

    protected Qualifier[][] populateQualifiers() throws StandardException {

        Qualifier[][] scanQualifiers = null;
        if (scanQualifiersField != null) {
            try {
                scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
            } catch (Exception e) {
                throw StandardException.unexpectedUserException(e);
            }
        }
        return scanQualifiers;
    }
    protected ExecIndexRow getStopPosition() throws StandardException {
        if (sameStartStopPosition)
            return null;
        if (stopKeyGetter == null && stopKeyGetterMethodName != null)
            stopKeyGetter = new SpliceMethod<>(stopKeyGetterMethodName, activation);

        return stopKeyGetter == null ? null : stopKeyGetter.invoke();
    }

    @Override
    public ExecIndexRow getStartPosition() throws StandardException {
        if (startKeyGetter == null && startKeyGetterMethodName != null)
            startKeyGetter = new SpliceMethod<>(startKeyGetterMethodName, activation);

        if (startKeyGetter != null)
            return startKeyGetter.invoke();
        return null;
    }


    @Override
    public List<DataScan> getScans(TxnView txn, ExecRow startKeyOverride, Activation activation, int[] keyDecodingMap) throws StandardException {
        throw new RuntimeException("getScans is not supported");
    }

    @Override
    public int[] getColumnOrdering() throws StandardException {
        return getConglomerate().getColumnOrdering();
    }

    @Override
    public int[] getIndexToBaseColumnMap() throws StandardException {
        if (this.indexColItem == -1)
            return Empty_Array;
        FormatableArrayHolder fah = (FormatableArrayHolder) activation.getPreparedStatement().getSavedObject(indexColItem);
        FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
        int[] keyColumns = new int[fihArray.length];
        for (int index = 0; index < fihArray.length; index++) {
            keyColumns[index] = fihArray[index].getInt() - 1; // 1 based to 0 based
        }
        return keyColumns;

    }

    @Override
    public boolean getSameStartStopPosition() {
        return sameStartStopPosition;
    }
}
