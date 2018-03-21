package com.splicemachine.derby.impl.storage;

import com.splicemachine.EngineDriver;
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
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.iapi.*;
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
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        Map<String, List<String>> errors = new HashMap<>();

        PairDataSet<String, byte[]> tableDataSet = getTableDataSet(dsp, heapConglomId);
        ExecRow key = getTableKeyExecRow(heapConglomId);
        KeyHashDecoder tableKeyDecoder = getKeyDecoder(key, null);
        TableChecker tableChecker = dsp.getTableChecker(schemaName, tableName, tableDataSet, tableKeyDecoder, key);

        // Check each index
        for(DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
            DDLMessage.Index index = tentativeIndex.getIndex();
            long indexConglom = index.getConglomerate();
            String indexName = getIndexName(cdList, index.getConglomerate());
            ExecRow indexKey = getIndexExecRow(indexConglom);
            List<Boolean> descColumnList = index.getDescColumnsList();
            boolean[] sortOrder = new boolean[descColumnList.size()];
            for (int i = 0; i < descColumnList.size(); ++i) {
                sortOrder[i] = !descColumnList.get(i);
            }
            KeyHashDecoder indexKeyDecoder = getKeyDecoder(indexKey, sortOrder);
            PairDataSet<String, byte[]> indexDataSet = getIndexDataSet(dsp, indexConglom, index.getUnique());

            List<String> messages = tableChecker.checkIndex(indexDataSet, indexName, indexKeyDecoder, indexKey);
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
    private PairDataSet<String, byte[]> getTableDataSet(DataSetProcessor dsp, long conglomerateId) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();

        int[] columnOrdering = conglomerate.getColumnOrdering();
        int maxCol = 0;
        for (int i = 0; i < columnOrdering.length; ++i) {
            if (columnOrdering[i] > maxCol)
                maxCol = columnOrdering[i];
        }
        int[] baseColumnMap = new int[0];
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(columnOrdering.length);
        ExecRow templateRow = new ValueRow(0);
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
                        .buildDataSet();

        PairDataSet<String, byte[]> dataSet = scanSet.index(new KeyByRowIdFunction());

        return dataSet;
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

    private String getIndexName(ConglomerateDescriptorList cds, long conglomerateNumber) {
        for (int i = 0; i < cds.size(); ++i) {
            ConglomerateDescriptor cd = cds.get(i);
            if (cd.getConglomerateNumber() == conglomerateNumber) {
                return cd.getObjectName();
            }
        }
        return null;
    }
}
