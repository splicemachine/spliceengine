package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.storage.*;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.si.impl.txn.WritableTxn;

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
        Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,RegionScanner,Result,Scan extends OperationWithAttributes> {

    public final SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib;
    private final String siNeededAttribute;
    private final String deletePutAttribute;
    private final TxnSupplier txnSupplier;
    private final TxnLifecycleManager control;
    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    private final byte[] siFail;
    private final byte[] userColumnFamily;
    private ExceptionFactory exceptionFactory;

    public DataStore(SDataLib dataLib,
                     String siNeededAttribute,
                     String deletePutAttribute,
                     byte[] siCommitQualifier,
                     byte[] siTombstoneQualifier,
                     byte[] siNull,
                     byte[] siAntiTombstoneValue,
                     byte[] siFail,
                     byte[] userColumnFamily,
                     TxnSupplier txnSupplier,
                     TxnLifecycleManager control,
                     ExceptionFactory exceptionFactory) {
        this.dataLib = dataLib;
        this.exceptionFactory = exceptionFactory;
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
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation,siNeededAttribute);
    }

    public byte[] getSINeededAttribute(Attributable operation) {
        return operation.getAttribute(siNeededAttribute);
    }


    public Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        byte[] neededValue = dataLib.getAttribute(operation,deletePutAttribute);
        if (neededValue == null) return false;
        return dataLib.decode(neededValue, Boolean.class);
    }

    public boolean getDeletePutAttribute(Attributable operation) {
        byte[] neededValue = operation.getAttribute(deletePutAttribute);
        if (neededValue == null) return false;
        return dataLib.decode(neededValue, Boolean.class);
    }

    public Txn getTxn(OperationWithAttributes operation, boolean readOnly) throws IOException {
        return decodeForOp(dataLib.getAttribute(operation,SI_TRANSACTION_KEY), readOnly);
    }


    public void checkBloom(Get get) {
        dataLib.setAttribute(get,CHECK_BLOOM_ATTRIBUTE_NAME, userColumnFamily);
    }

    public boolean isAntiTombstone(Data keyValue) {
        return dataLib.isAntiTombstone(keyValue, siAntiTombstoneValue);
    }

    public CellType getKeyValueType(Data keyValue) {
        if (dataLib.singleMatchingQualifier(keyValue, commitTimestampQualifier)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (dataLib.singleMatchingQualifier(keyValue, tombstoneQualifier)) {
            if (dataLib.matchingValue(keyValue, siNull)) {
                return CellType.TOMBSTONE;
            } else if (dataLib.matchingValue(keyValue, siAntiTombstoneValue)) {
                return CellType.ANTI_TOMBSTONE;
            }
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
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

    public void setTombstonesOnColumns(Partition table, long timestamp, DataPut put) throws IOException {
        //-sf- this doesn't really happen in practice, it's just for a safety valve, which is good, cause it's expensive
        final Map<byte[], byte[]> userData = getUserData(table,put.key());
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                put.addCell(userColumnFamily,qualifier,timestamp,siNull);
            }
        }
    }

    public void setAntiTombstoneOnPut(Put put, long transactionId) throws IOException {
        //if (LOG.isTraceEnabled()) LOG.trace(String.format("Flipping on anti-tombstone column: put = %s, txnId = %s, stackTrace = %s", put, transactionId, Arrays.toString(Thread.currentThread().getStackTrace()).replaceAll(", ", "\n\t")));
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
    }

    private Map<byte[], byte[]> getUserData(Partition table, byte[] rowKey) throws IOException {
        DataResult dr = table.getLatest(rowKey,userColumnFamily,null);
        if (dr != null) {
            return dr.familyCellMap(userColumnFamily);
        }
        return null;
    }

    public String getTableName(Partition table) {
        return table.getName();
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
            return new WritableTxn(txnId, beginTs, level, txnSupplier.getTransaction(parentTxnId), control, false,exceptionFactory);
        }
    }

    public SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> getDataLib() {
        return this.dataLib;
    }
}
