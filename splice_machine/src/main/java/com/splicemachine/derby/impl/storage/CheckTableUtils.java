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
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import com.splicemachine.derby.utils.SpliceTableAdmin;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class CheckTableUtils {

    private static Logger LOG=Logger.getLogger(CheckTableUtils.class);

    public static Map<String, List<String>> checkTable(String schemaName, String tableName, TableDescriptor td,
                                                       List<DDLMessage.TentativeIndex> tentativeIndexList,
                                                       long heapConglomId,
                                                       boolean distributed,
                                                       boolean fix,
                                                       boolean isSystemTable,
                                                       TxnView txn,
                                                       Activation activation,
                                                       String jobGroup) throws Exception{
        Map<String, List<String>> errors = new TreeMap<>();

        DataSetProcessor dsp = null;
        if (distributed) {
            SpliceLogUtils.info(LOG, "Run check_table on spark");
            dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        } else {
            SpliceLogUtils.info(LOG, "Run check_table on region server");
            dsp = EngineDriver.driver().processorFactory().localProcessor(null, null);
        }

        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(jobGroup, "");

        int[] baseColumnMap = getBaseColumnMap(tentativeIndexList);
        DataSet<ExecRow> tableDataSet = getTableDataSet(dsp, heapConglomId, tentativeIndexList, baseColumnMap,
                activation, tableName, txn, td);
        ExecRow key = getTableKeyExecRow(heapConglomId, activation);
        KeyHashDecoder tableKeyDecoder = getKeyDecoder(td, key, null);
        TableChecker tableChecker = dsp.getTableChecker(schemaName, tableName, tableDataSet,
                tableKeyDecoder, key, txn, fix, baseColumnMap, isSystemTable);

        Map<Long, CheckTableUtils.LeadingIndexColumnInfo> leadingIndexColumnInfoMap = getLeadingIndexColumnMap(tentativeIndexList, baseColumnMap);
        ConglomerateDescriptorList  cdList = td.getConglomerateDescriptorList();
        // Check each index
        for(DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
            DDLMessage.Index index = tentativeIndex.getIndex();
            long indexConglom = index.getConglomerate();
            String indexName = SpliceTableAdmin.getIndexName(cdList, index.getConglomerate());
            PairDataSet<String, Tuple2<byte[], ExecRow>> indexDataSet = getIndexDataSet(dsp, tableName, indexConglom,
                    index.getUnique(), activation, txn, td.getVersion());

            CheckTableUtils.LeadingIndexColumnInfo leadingIndexColumnInfo = null;
            if (index.getExcludeDefaults() || index.getExcludeNulls()) {
                long conglomerate = index.getConglomerate();
                leadingIndexColumnInfo = leadingIndexColumnInfoMap.get(conglomerate);
            }
            if (!distributed) {
                // Create a new dataset for table if it is not checked on spark, because the dataset is essentially an
                // iterator. Each time the table is checked, the iterator is consumed.
                tableDataSet = getTableDataSet(dsp, heapConglomId, tentativeIndexList, baseColumnMap, activation,
                        tableName, txn, td);
                tableChecker.setTableDataSet(tableDataSet);
            }
            List<String> messages = tableChecker.checkIndex(indexDataSet, indexName, leadingIndexColumnInfo,
                    index.getConglomerate(), tentativeIndex);
            if (messages.size() > 0) {
                errors.put(indexName, messages);
            }
        }
        return errors;
    }

    /**
     * Get key decoder for base table
     * @param execRow
     * @param sortOrder
     * @return
     * @throws Exception
     */
    private static KeyHashDecoder getKeyDecoder(TableDescriptor td, ExecRow execRow, boolean[] sortOrder) throws Exception {

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
    private static ExecRow getTableKeyExecRow(long conglomerateId, Activation activation) throws StandardException {

        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
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
     * Construct a DataSet for index
     * @param dsp
     * @param conglomerateId
     * @param isUnique
     * @return
     * @throws StandardException
     */
    private static PairDataSet<String, Tuple2<byte[], ExecRow>> getIndexDataSet(DataSetProcessor dsp,
                                                                                String tableName,
                                                                                long conglomerateId,
                                                                                boolean isUnique,
                                                                                Activation activation,
                                                                                TxnView txn,
                                                                                String tableVersion) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();
        int[] columnOrdering = IntArrays.count(isUnique? formatIds.length - 1 :formatIds.length);
        int[] baseColumnMap = new int[formatIds.length];
        for (int i = 0; i < formatIds.length; ++i) {
            baseColumnMap[i] = i;
        }
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(formatIds.length);
        for (int i = 0; i < formatIds.length; ++i) {
            accessedKeyColumns.set(i);
        }
        ExecRow templateRow = new ValueRow(formatIds.length);
        DataValueDescriptor[] dvds = templateRow.getRowArray();
        DataValueFactory dataValueFactory=lcc.getDataValueFactory();
        for (int i = 0 ; i < formatIds.length; ++i) {
            dvds[i] = dataValueFactory.getNull(formatIds[i], -1);
        }
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

        PairDataSet<String, Tuple2<byte[], ExecRow>> dataSet = scanSet.index(new KeyByBaseRowIdFunction());

        return dataSet;
    }

    /**
     * Construct a DataSet for base table
     * @param dsp
     * @param conglomerateId
     * @return
     * @throws StandardException
     */
    private static DataSet<ExecRow> getTableDataSet(DataSetProcessor dsp, long conglomerateId,
                                                    List<DDLMessage.TentativeIndex> tentativeIndexList,
                                                    int[] baseColumnMap,
                                                    Activation activation,
                                                    String tableName,
                                                    TxnView txn,
                                                    TableDescriptor td) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        int[] formatIds = conglomerate.getFormat_ids();
        int[] columnOrdering = conglomerate.getColumnOrdering();
        FormatableBitSet accessedKeyColumns = getAccessedKeyColumns(columnOrdering, baseColumnMap);
        ExecRow templateRow = getTemplateRow(lcc, baseColumnMap, formatIds);
        ExecRow defaultValueRow = getDefaultValueRow(td, baseColumnMap, templateRow);
        FormatableBitSet defaultValueMap = getDefaultValueMap(defaultValueRow);

        DataSet<ExecRow> scanSet =
                dsp.<TableScanOperation,ExecRow>newScanSet(null, Long.toString(conglomerateId))
                        .activation(activation)
                        .tableDisplayName(tableName)
                        .transaction(txn)
                        .scan(createScan(txn))
                        .tableVersion(td.getVersion())
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

    private static FormatableBitSet getAccessedKeyColumns(int[] columnOrdering, int[] baseColumnMap) {
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(columnOrdering.length);
        for (int i = 0; i < baseColumnMap.length; ++i) {
            if (baseColumnMap[i] != -1) {
                for (int j = 0; j < columnOrdering.length; ++j) {
                    if (columnOrdering[j] == i) {
                        accessedKeyColumns.set(j);
                    }
                }
            }
        }
        return accessedKeyColumns;
    }

    private static FormatableBitSet getDefaultValueMap(ExecRow defaultValueRow) throws StandardException {
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
    private static ExecRow getDefaultValueRow(TableDescriptor td, int[] baseColumnMap, ExecRow templateRow) {
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

    private static Map<Long, CheckTableUtils.LeadingIndexColumnInfo> getLeadingIndexColumnMap(List<DDLMessage.TentativeIndex> tentativeIndexList, int[] baseColumnMap) {

        Map<Long, CheckTableUtils.LeadingIndexColumnInfo> leadingIndexColumnInfoMap = new HashMap<>();
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
                leadingIndexColumnInfoMap.put(conglomerate, new CheckTableUtils.LeadingIndexColumnInfo(pos, excludeNulls, excludeDefaults, defaultValue));
            }
        }

        return leadingIndexColumnInfoMap;
    }

    private static ExecRow getTemplateRow(LanguageConnectionContext lcc, int[] baseColumnMap, int[] formatIds) throws StandardException {

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

    private static int[] getBaseColumnMap(List<DDLMessage.TentativeIndex> tentativeIndexList) {
        // includes all indexed columns
        BitSet accessedCols = new BitSet();
        int max = 0;
        int[] baseColumnMap = new int[0];
        for(DDLMessage.TentativeIndex index : tentativeIndexList) {
            List<Integer> indexCols = index.getIndex().getIndexColsToMainColMapList();
            for (int indexCol : indexCols) {
                if (indexCol > max) {
                    max = indexCol;
                }
                accessedCols.set(indexCol - 1);
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
    private static DataScan createScan (TxnView txn) {
        DataScan scan= SIDriver.driver().getOperationFactory().newDataScan(txn);
        //exclude this from SI treatment, since we're doing it internally
        scan.addAttribute(SIConstants.SI_NEEDED,null);
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

        private CheckTableUtils.LeadingIndexColumnInfo leadingIndexColumnInfo;

        public IndexFilter(){}

        public IndexFilter(CheckTableUtils.LeadingIndexColumnInfo leadingIndexColumnInfo) {
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
            leadingIndexColumnInfo = (CheckTableUtils.LeadingIndexColumnInfo)in.readObject();
        }
    }
}
