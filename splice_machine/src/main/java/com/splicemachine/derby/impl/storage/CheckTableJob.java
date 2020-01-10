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

package com.splicemachine.derby.impl.storage;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.utils.SpliceTableAdmin;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 2/5/18.
 */
public class CheckTableJob implements Callable<Void> {

    private static Logger LOG=Logger.getLogger(CheckTableJob.class);

    private final OlapStatus jobStatus;
    private final DistributedCheckTableJob request;
    private String tableName;
    private String schemaName;
    private Activation activation;
    private TxnView txn;
    private DataDictionary dd;
    private List<DDLMessage.TentativeIndex> tentativeIndexList;
    private long heapConglomId;
    private TableDescriptor td;
    private ConglomerateDescriptorList cdList;
    private DDLMessage.Table table;
    LanguageConnectionContext lcc;
    private String tableVersion;
    private Map<Long, LeadingIndexColumnInfo> leadingIndexColumnInfoMap;

    public CheckTableJob(DistributedCheckTableJob request,OlapStatus jobStatus) {
        this.jobStatus = jobStatus;
        this.request = request;
    }

    @Override
    public Void call() throws Exception {
        if(!jobStatus.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }
        init();

        String table = Long.toString(heapConglomId);
        Collection<PartitionLoad> partitionLoadCollection = EngineDriver.driver().partitionLoadWatcher().tableLoad(table, true);
        boolean distributed = false;
        for (PartitionLoad load: partitionLoadCollection) {
            if (load.getMemStoreSizeMB() > 0 || load.getStorefileSizeMB() > 0)
                distributed = true;
        }
        DataSetProcessor dsp = null;
        if (distributed) {
            SpliceLogUtils.info(LOG, "Run check_table on spark");
            dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        }
        else {
            SpliceLogUtils.info(LOG, "Run check_table on region server");
            dsp = EngineDriver.driver().processorFactory().localProcessor(null, null);
        }

        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.jobGroup, "");

        CheckTableResult checkTableResult = new CheckTableResult();
        Map<String, List<String>> errors = new TreeMap<>();

        DataSet<ExecRow> tableDataSet = getTableDataSet(dsp, heapConglomId, tentativeIndexList);
        ExecRow key = getTableKeyExecRow(heapConglomId);
        KeyHashDecoder tableKeyDecoder = getKeyDecoder(key, null);
        TableChecker tableChecker = dsp.getTableChecker(schemaName, tableName, tableDataSet, tableKeyDecoder, key);

        // Check each index
        for(DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
            DDLMessage.Index index = tentativeIndex.getIndex();
            long indexConglom = index.getConglomerate();
            String indexName = SpliceTableAdmin.getIndexName(cdList, index.getConglomerate());
            ExecRow indexKey = getIndexExecRow(indexConglom);
            List<Boolean> descColumnList = index.getDescColumnsList();
            boolean[] sortOrder = new boolean[descColumnList.size()];
            for (int i = 0; i < descColumnList.size(); ++i) {
                sortOrder[i] = !descColumnList.get(i);
            }
            KeyHashDecoder indexKeyDecoder = getKeyDecoder(indexKey, sortOrder);
            PairDataSet<String, byte[]> indexDataSet = getIndexDataSet(dsp, indexConglom, index.getUnique());

            LeadingIndexColumnInfo leadingIndexColumnInfo = null;
            if (index.getExcludeDefaults() || index.getExcludeNulls()) {
                long conglomerate = index.getConglomerate();
                leadingIndexColumnInfo = leadingIndexColumnInfoMap.get(conglomerate);
            }
            if (!distributed) {
                // Create a new dataset for table if it is not checked on spark, because the dataset is essentially an
                // iterator. Each time the table is checked, the iterator is consumed.
                tableDataSet = getTableDataSet(dsp, heapConglomId, tentativeIndexList);
                tableChecker.setTableDataSet(tableDataSet);
            }
            List<String> messages = tableChecker.checkIndex(indexDataSet, indexName, indexKeyDecoder, indexKey, leadingIndexColumnInfo);
            if (messages.size() > 0) {
                errors.put(indexName, messages);
            }
        }

        if (errors.size() > 0) {
            checkTableResult.setResults(errors);
        }

        jobStatus.markCompleted(checkTableResult);
        return null;
    }

    /**
     * Get key decoder for base table
     * @param execRow
     * @param sortOrder
     * @return
     * @throws Exception
     */
    private KeyHashDecoder getKeyDecoder(ExecRow execRow, boolean[] sortOrder) throws Exception {

        String version = td.getVersion();
        DescriptorSerializer[] serializers= VersionedSerializers
                .forVersion(version,true)
                .getSerializers(execRow.getRowArray());
        int[] rowColumns = IntArrays.count(execRow.nColumns());
        DataHash dataHash = BareKeyHash.encoder(rowColumns, sortOrder, serializers);
        return dataHash.getDecoder();
    }

    /**
     * Get row template for base table key
     * @param conglomerateId
     * @return
     * @throws StandardException
     */
    private ExecRow getTableKeyExecRow(long conglomerateId) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();

        int[] columnOrdering = conglomerate.getColumnOrdering();
        ExecRow key = new ValueRow(columnOrdering.length);
        if (columnOrdering.length > 0) {
            DataValueDescriptor[] dvds = key.getRowArray();
            DataValueFactory dataValueFactory = lcc.getDataValueFactory();
            for (int i = 0; i < columnOrdering.length; i++) {
                dvds[i] = dataValueFactory.getNull(formatIds[columnOrdering[i]], -1);
            }
        }

        return key;
    }

    /**
     * Get Index row template
     * @param conglomerateId
     * @return
     * @throws StandardException
     */
    private ExecRow getIndexExecRow(long conglomerateId) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();
        ExecRow key = new ValueRow(formatIds.length-1);
        if (formatIds.length > 0) {
            DataValueDescriptor[] dvds = key.getRowArray();
            DataValueFactory dataValueFactory = lcc.getDataValueFactory();
            for (int i = 0; i < formatIds.length-1; i++) {
                dvds[i] = dataValueFactory.getNull(formatIds[i], -1);
            }
        }
        return key;
    }

    /**
     * Construct a DataSet for index
     * @param dsp
     * @param conglomerateId
     * @param isUnique
     * @return
     * @throws StandardException
     */
    private PairDataSet<String, byte[]> getIndexDataSet(DataSetProcessor dsp, long conglomerateId, boolean isUnique) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();
        int[] columnOrdering = IntArrays.count(isUnique? formatIds.length - 1 :formatIds.length);
        int[] baseColumnMap = new int[formatIds.length];
        for (int i = 0; i < formatIds.length; ++i) {
            baseColumnMap[i] = -1;
        }
        // Only decode last column of an index row, which is the row address of base table
        baseColumnMap[formatIds.length-1] = 0;
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(formatIds.length);
        accessedKeyColumns.set(formatIds.length-1);
        ExecRow templateRow = new ValueRow(1);
        DataValueDescriptor[] dvds = templateRow.getRowArray();
        DataValueFactory dataValueFactory=lcc.getDataValueFactory();
        dvds[0] = dataValueFactory.getNull(formatIds[formatIds.length-1],-1);


        DataSet<ExecRow> scanSet =
                dsp.<TableScanOperation,ExecRow>newScanSet(null, Long.toString(conglomerateId))
                        .activation(activation)
                        .tableDisplayName(tableName)
                        .transaction(txn)
                        .scan(createScan(txn))
                        .tableVersion(tableVersion)
                        .template(templateRow)
                        .keyColumnEncodingOrder(columnOrdering)
                        .keyColumnTypes(ScanOperation.getKeyFormatIds(columnOrdering, formatIds))
                        .accessedKeyColumns(accessedKeyColumns)
                        .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .baseColumnMap(columnOrdering)
                        .buildDataSet();

        PairDataSet<String, byte[]> dataSet = scanSet.index(new KeyByBaseRowIdFunction());

        return dataSet;
    }

    /**
     * Construct a DataSet for base table
     * @param dsp
     * @param conglomerateId
     * @return
     * @throws StandardException
     */
    private DataSet<ExecRow> getTableDataSet(DataSetProcessor dsp, long conglomerateId,
                                                        List<DDLMessage.TentativeIndex> tentativeIndexList) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();
        int[] columnOrdering = conglomerate.getColumnOrdering();
        int[] baseColumnMap = getBaseColumnMap(tentativeIndexList);
        FormatableBitSet accessedKeyColumns = getAccessedKeyColumns(columnOrdering, baseColumnMap);
        ExecRow templateRow = getTemplateRow(baseColumnMap, formatIds);
        ExecRow defaultValueRow = getDefaultValueRow(baseColumnMap, templateRow);
        FormatableBitSet defaultValueMap = getDefaultValueMap(defaultValueRow);

        this.leadingIndexColumnInfoMap = getIndexColumnMap(tentativeIndexList, baseColumnMap);
        DataSet<ExecRow> scanSet =
                dsp.<TableScanOperation,ExecRow>newScanSet(null, Long.toString(conglomerateId))
                        .activation(activation)
                        .tableDisplayName(tableName)
                        .transaction(txn)
                        .scan(createScan(txn))
                        .tableVersion(tableVersion)
                        .template(templateRow)
                        .keyColumnEncodingOrder(columnOrdering)
                        .keyColumnTypes(ScanOperation.getKeyFormatIds(columnOrdering, formatIds))
                        .accessedKeyColumns(accessedKeyColumns)
                        .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .baseColumnMap(baseColumnMap)
                        .defaultRow(defaultValueRow, defaultValueMap)
                        .buildDataSet();

        return scanSet;
    }

    private FormatableBitSet getAccessedKeyColumns(int[] columnOrdering, int[] baseColumnMap) {
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(columnOrdering.length);
        for (int i = 0; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] != -1) {
                int pos = baseColumnMap[i];
                for (int j = 0; j < columnOrdering.length; ++j) {
                    if (columnOrdering[j] == pos) {
                        accessedKeyColumns.set(j);
                    }
                }
            }
        }
        return accessedKeyColumns;
    }

    private FormatableBitSet getDefaultValueMap(ExecRow defaultValueRow) throws StandardException {
        FormatableBitSet defaultValueMap = null;
        int size = defaultValueRow.size();
        int index = 0;
        if (size > 0) {
            defaultValueMap = new FormatableBitSet(size);
            for (int i = 0; i < defaultValueRow.nColumns(); ++i) {
                if (defaultValueRow.getColumn(i+1) != null && !defaultValueRow.getColumn(i+1).isNull()) {
                    defaultValueMap.set(index);
                }
                index++;
            }
        }
        return defaultValueMap;
    }
    private ExecRow getDefaultValueRow(int[] baseColumnMap, ExecRow templateRow) {
        ExecRow defaultValueRow = templateRow.getClone();
        DataValueDescriptor[] dvds = defaultValueRow.getRowArray();
        ColumnDescriptorList cdl=td.getColumnDescriptorList();
        int index = 0;
        for (int i = 0; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] >= 0) {
                ColumnDescriptor cd = cdl.get(i);
                boolean nullable = cd.getType().isNullable();
                DefaultInfoImpl defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();
                if (!nullable && defaultInfo != null) {
                    dvds[index] = defaultInfo.getDefaultValue();
                }
                index++;
            }
        }
        return defaultValueRow;
    }
    private Map<Long, LeadingIndexColumnInfo> getIndexColumnMap(List<DDLMessage.TentativeIndex> tentativeIndexList, int[] baseColumnMap) {

        Map<Long, LeadingIndexColumnInfo> leadingIndexColumnInfoMap = new HashMap<>();
        for (DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
            boolean excludeNulls = tentativeIndex.getIndex().hasExcludeNulls() && tentativeIndex.getIndex().getExcludeNulls();
            boolean excludeDefaults = tentativeIndex.getIndex().hasExcludeDefaults() && tentativeIndex.getIndex().getExcludeDefaults();

            // If exclude default and default is null, this is equivalent to exclude null
            if (excludeDefaults && tentativeIndex.getIndex().getDefaultValues().size() == 0) {
                excludeDefaults = false;
                excludeNulls=true;
            }

            if (excludeDefaults || excludeNulls) {
                List<Integer> indexCols = tentativeIndex.getIndex().getIndexColsToMainColMapList();
                int indexCol = indexCols.get(0)-1;
                // Find the position in template row
                int pos = baseColumnMap[indexCol];
                DataValueDescriptor defaultValue = null;
                if (excludeDefaults) {
                    defaultValue = (DataValueDescriptor) SerializationUtils.deserialize(tentativeIndex.getIndex().getDefaultValues().toByteArray());
                }
                Long conglomerate = tentativeIndex.getIndex().getConglomerate();
                leadingIndexColumnInfoMap.put(conglomerate, new LeadingIndexColumnInfo(pos, excludeNulls, excludeDefaults, defaultValue));
            }
        }

        return leadingIndexColumnInfoMap;
    }

    private ExecRow getTemplateRow(int[] baseColumnMap, int[] formatIds) throws StandardException {

        int size = 0;
        for(int i = 0 ; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] != -1)
                size++;
        }
        DataValueFactory dataValueFactory = lcc.getDataValueFactory();
        ExecRow execRow = new ValueRow(size);
        DataValueDescriptor[] dvds = execRow.getRowArray();
        int index = 0;
        for (int i = 0; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] != -1) {
                dvds[index++] = dataValueFactory.getNull(formatIds[i], -1);
            }
        }
        return execRow;
    }

    private int[] getBaseColumnMap(List<DDLMessage.TentativeIndex> tentativeIndexList) {

        BitSet accessedCols = new BitSet();
        // Only include the first column of indexes that excludes null or default values
        int max = 0;
        int[] baseColumnMap = new int[0];
        for(DDLMessage.TentativeIndex index : tentativeIndexList) {
            boolean excludeNulls = index.getIndex().hasExcludeNulls() && index.getIndex().getExcludeNulls();
            boolean excludeDefaults = index.getIndex().hasExcludeDefaults() && index.getIndex().getExcludeDefaults();
            if (excludeNulls || excludeDefaults) {
                List<Integer> indexCols = index.getIndex().getIndexColsToMainColMapList();
                int indexCol = indexCols.get(0);
                if (indexCol > max) {
                    max = indexCol;
                }
                accessedCols.set(indexCol-1);
            }
        }
        if (accessedCols.cardinality() > 0) {
            baseColumnMap = new int[max];
            Arrays.fill(baseColumnMap, -1);
            int index = 0;
            for (int i = accessedCols.nextSetBit(0); i != -1; i = accessedCols.nextSetBit(i + 1)) {
                baseColumnMap[i] = index++;
            }
        }
        return baseColumnMap;
    }

    private void init() throws StandardException, SQLException{
        tentativeIndexList = request.tentativeIndexList;
        schemaName = request.schemaName;
        tableName = request.tableName;
        activation = request.ah.getActivation();
        txn = request.txn;
        table = tentativeIndexList.get(0).getTable();
        heapConglomId = table.getConglomerate();
        lcc = activation.getLanguageConnectionContext();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        dd =lcc.getDataDictionary();

        SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
        boolean prepared = false;
        try {
            prepared=transactionResource.marshallTransaction(txn);
            SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
            td = dd.getTableDescriptor(tableName, sd, tc);
            cdList = td.getConglomerateDescriptorList();
            tableVersion = td.getVersion();
        }
        finally {
            if (prepared)
                transactionResource.close();
        }
    }

    private DataScan createScan (TxnView txn) {
        DataScan scan= SIDriver.driver().getOperationFactory().newDataScan(txn);
        scan.returnAllVersions(); //make sure that we read all versions of the data
        return scan.startKey(new byte[0]).stopKey(new byte[0]);
    }

    public static class LeadingIndexColumnInfo implements Externalizable {
        private int position;
        private boolean excludeNulls;
        private boolean excludeDefaults;
        private DataValueDescriptor defaultValue;

        public LeadingIndexColumnInfo() {}

        public LeadingIndexColumnInfo(int position,
                                      boolean excludeNulls,
                                      boolean excludeDefaults,
                                      DataValueDescriptor defaultValue){
            this.position = position;
            this.excludeNulls = excludeNulls;
            this.excludeDefaults = excludeDefaults;
            this.defaultValue = defaultValue;
        }

        public int getPosition(){
            return position;
        }

        public boolean excludeNulls() {
            return excludeNulls;
        }

        public boolean excludeDefaults(){
            return excludeDefaults;
        }

        public DataValueDescriptor getDefaultValue() {
            return defaultValue;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(position);
            out.writeBoolean(excludeNulls);
            out.writeBoolean(excludeDefaults);
            out.writeBoolean(defaultValue!=null);
            if (defaultValue!=null) {
                out.writeObject(defaultValue);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            position = in.readInt();
            excludeNulls = in.readBoolean();
            excludeDefaults = in.readBoolean();
            if (in.readBoolean()){
                defaultValue = (DataValueDescriptor) in.readObject();
            }
        }
    }

    public static class IndexFilter <Op extends SpliceOperation> extends SplicePredicateFunction<Op,ExecRow> {

        private LeadingIndexColumnInfo leadingIndexColumnInfo;

        public IndexFilter(){}

        public IndexFilter(LeadingIndexColumnInfo leadingIndexColumnInfo) {
            this.leadingIndexColumnInfo = leadingIndexColumnInfo;
        }

        @Override
        public boolean apply(@Nullable ExecRow row) {
            try {
                if (leadingIndexColumnInfo == null)
                    return true;
                else {
                    int position = leadingIndexColumnInfo.getPosition();
                    DataValueDescriptor[] dvds = row.getRowArray();
                    if (leadingIndexColumnInfo.excludeNulls()) {
                        return !dvds[position].isNull();
                    } else if (leadingIndexColumnInfo.excludeDefaults()) {
                        return dvds[position].compare(leadingIndexColumnInfo.getDefaultValue()) != 0;
                    }
                }
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeObject(leadingIndexColumnInfo);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            leadingIndexColumnInfo = (LeadingIndexColumnInfo)in.readObject();
        }
    }
}
