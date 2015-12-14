package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.procedures.LongProcedure;
import com.splicemachine.si.api.txn.KeyValueType;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.STableReader;
import com.splicemachine.si.api.data.STableWriter;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static com.splicemachine.si.constants.SIConstants.SI_TRANSACTION_KEY;
import static com.splicemachine.si.constants.SIConstants.CHECK_BLOOM_ATTRIBUTE_NAME;
import static com.splicemachine.si.constants.SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.si.constants.SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */


public class DataStore<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Mutation,OperationStatus,
        Put extends OperationWithAttributes,RegionScanner,Result,ReturnCode,RowLock,Scan extends OperationWithAttributes,Table> {

    public final SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib;
    private final STableReader<Table, Get, Scan,Result> reader;
    private final STableWriter<Delete,Mutation,OperationStatus,Put,RowLock,Table> writer;
    private final String siNeededAttribute;
    private final String deletePutAttribute;
    private final TxnSupplier txnSupplier;
    private final TxnLifecycleManager control;
    private final List<List<byte[]>> userFamilyColumnList;
    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    private final byte[] siFail;
    private final byte[] userColumnFamily;

    public DataStore(SDataLib dataLib,
                     STableReader reader,
                     STableWriter writer,
                     String siNeededAttribute,
                     String deletePutAttribute,
                     byte[] siCommitQualifier,
                     byte[] siTombstoneQualifier,
                     byte[] siNull,
                     byte[] siAntiTombstoneValue,
                     byte[] siFail,
                     byte[] userColumnFamily,
                     TxnSupplier txnSupplier,
                     TxnLifecycleManager control) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.commitTimestampQualifier = siCommitQualifier;
        this.tombstoneQualifier = siTombstoneQualifier;
        this.siNull = siNull;
        this.siAntiTombstoneValue = siAntiTombstoneValue;
        this.siFail = siFail;
        this.userColumnFamily = userColumnFamily;
        this.txnSupplier = txnSupplier;
        this.control = control;
        this.userFamilyColumnList = Arrays.asList(
                Arrays.asList(this.userColumnFamily, tombstoneQualifier),
                Arrays.asList(this.userColumnFamily, commitTimestampQualifier),
                Arrays.asList(this.userColumnFamily, SIConstants.PACKED_COLUMN_BYTES),
                Arrays.asList(this.userColumnFamily, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)
        );
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation,siNeededAttribute);
    }


    public Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        byte[] neededValue = dataLib.getAttribute(operation,deletePutAttribute);
        if (neededValue == null) return false;
        return dataLib.decode(neededValue, Boolean.class);
    }

    public Txn getTxn(OperationWithAttributes operation, boolean readOnly) throws IOException {
        return decodeForOp(dataLib.getAttribute(operation,SI_TRANSACTION_KEY), readOnly);
    }

    public Delete copyPutToDelete(final Put put, LongOpenHashSet transactionIdsToDelete) {
        final Delete delete = dataLib.newDelete(dataLib.getPutKey(put));
        final Iterable<Data> cells = dataLib.listPut(put);
        transactionIdsToDelete.forEach(new LongProcedure() {
            @Override
            public void apply(long transactionId) {
                for (Data data : cells) {
                    dataLib.addDataToDelete(delete, data, transactionId);
                }
                dataLib.addFamilyQualifierToDelete(delete, userColumnFamily, tombstoneQualifier, transactionId);
                dataLib.addFamilyQualifierToDelete(delete, userColumnFamily, commitTimestampQualifier, transactionId);

            }
        });
        return delete;
    }

    public Result getCommitTimestampsAndTombstonesSingle(Table table, byte[] rowKey) throws IOException {
        Get get = dataLib.newGet(rowKey, null, userFamilyColumnList, null, 1); // Just Retrieve one per...
        suppressIndexing(get);
        checkBloom(get);
        return reader.get(table, get);
    }

    public void checkBloom(Get get) {
        dataLib.setAttribute(get,CHECK_BLOOM_ATTRIBUTE_NAME, userColumnFamily);
    }

    public boolean isAntiTombstone(Data keyValue) {
        return dataLib.isAntiTombstone(keyValue, siAntiTombstoneValue);
    }

    public KeyValueType getKeyValueType(Data keyValue) {
        if (dataLib.singleMatchingQualifier(keyValue, commitTimestampQualifier)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return KeyValueType.USER_DATA;
        } else if (dataLib.singleMatchingQualifier(keyValue, tombstoneQualifier)) {
            if (dataLib.matchingValue(keyValue, siNull)) {
                return KeyValueType.TOMBSTONE;
            } else if (dataLib.matchingValue(keyValue, siAntiTombstoneValue)) {
                return KeyValueType.ANTI_TOMBSTONE;
            }
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return KeyValueType.FOREIGN_KEY_COUNTER;
        }
        return KeyValueType.OTHER;
    }

    public boolean isSINull(Data keyValue) {
        return dataLib.matchingValue(keyValue, siNull);
    }

    public boolean isSIFail(Data keyValue) {
        return dataLib.matchingValue(keyValue, siFail);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(OperationWithAttributes operation) {
        dataLib.setAttribute(operation,SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public boolean isSuppressIndexing(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation,SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null;
    }

    public void setTombstoneOnPut(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siNull);
    }

    public void setTombstonesOnColumns(Table table, long timestamp, Put put) throws IOException {
        final Map<byte[], byte[]> userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    public void setAntiTombstoneOnPut(Put put, long transactionId) throws IOException {
        //if (LOG.isTraceEnabled()) LOG.trace(String.format("Flipping on anti-tombstone column: put = %s, txnId = %s, stackTrace = %s", put, transactionId, Arrays.toString(Thread.currentThread().getStackTrace()).replaceAll(", ", "\n\t")));
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
    }

    private Map<byte[], byte[]> getUserData(Table table, byte[] rowKey) throws IOException {
        final List<byte[]> families = Arrays.asList(userColumnFamily);
        Get get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setGetMaxVersions(get, 1);
        Result result = reader.get(table, get);
        if (result != null) {
            return dataLib.getFamilyMap(result,userColumnFamily);
        }
        return null;
    }

    public OperationStatus[] writeBatch(Table table, Pair[] mutationsAndLocks) throws IOException {
        return writer.writeBatch(table, mutationsAndLocks);
    }

    public String getTableName(Table table) {
        return reader.getTableName(table);
    }

    private Txn decodeForOp(byte[] txnData, boolean readOnly) throws IOException {
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(txnData);
        long txnId = decoder.decodeNextLong();
        long parentTxnId = decoder.readOrSkipNextLong(-1l);
        long beginTs = decoder.decodeNextLong();
        Txn.IsolationLevel level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());

        if (readOnly)
            return ReadOnlyTxn.createReadOnlyTransaction(txnId,
                    txnSupplier.getTransaction(parentTxnId), beginTs, level, false, control);
        else {
            return new WritableTxn(txnId, beginTs, level, txnSupplier.getTransaction(parentTxnId), control, false);
        }
    }

    public SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> getDataLib() {
        return this.dataLib;
    }
}
