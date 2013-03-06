package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.IdSource;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SiTransactor implements Transactor, ClientTransactor {
    private final IdSource idSource;
    private final SDataLib dataLib;
    private final STableWriter dataWriter;
    private final RowMetadataStore rowMetadataStore;
    private final TransactionStore transactionStore;

    public SiTransactor(IdSource idSource, SDataLib dataLib, STableWriter dataWriter,
                        RowMetadataStore rowMetadataStore, TransactionStore transactionStore) {
        this.idSource = idSource;
        this.dataLib = dataLib;
        this.dataWriter = dataWriter;
        this.rowMetadataStore = rowMetadataStore;
        this.transactionStore = transactionStore;
    }

    @Override
    public TransactionId beginTransaction() {
        final SiTransactionId transactionId = new SiTransactionId(idSource.nextId());
        transactionStore.recordNewTransaction(transactionId, TransactionStatus.ACTIVE);
        return transactionId;
    }

    @Override
    public void commit(TransactionId transactionId) {
        TransactionStruct transaction = transactionStore.getTransactionStatus(transactionId);
        if (!transaction.status.equals(TransactionStatus.ACTIVE)) {
            throw new RuntimeException("transaction is not ACTIVE");
        }
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.COMMITTING);
        final long endId = idSource.nextId();
        transactionStore.recordTransactionCommit(transactionId, endId, TransactionStatus.COMMITED);
    }

    @Override
    public void abort(TransactionId transactionId) {
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ABORT);
    }

    @Override
    public void fail(TransactionId transactionId) {
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ERROR);
    }

    @Override
    public void initializePuts(List puts) {
        for (Object put : puts) {
            rowMetadataStore.setSiNeededAttribute(put);
        }
    }

    @Override
    public void processPuts(TransactionId transactionId, STable table, List puts) {
        List nonSiPuts = new ArrayList();
        for (Object put : puts) {
            Boolean siNeeded = rowMetadataStore.getSiNeededAttribute(put);
            if (siNeeded) {
                Object row = dataLib.getPutKey(put);
                SRowLock lock = dataWriter.lockRow(table, row);
                try {
                    checkForConflict(transactionId, table, lock, row);
                    Object newPut = rowMetadataStore.clonePut(transactionId, put, row, lock);
                    rowMetadataStore.addTransactionIdToPut(newPut, transactionId);
                    dataWriter.write(table, Arrays.asList(newPut));
                } finally {
                    dataWriter.unLockRow(table, lock);
                }
            } else {
                nonSiPuts.add(put);
            }
        }
        dataWriter.write(table, nonSiPuts);
    }

    private void checkForConflict(TransactionId transactionId, STable table, SRowLock lock, Object row) {
        long id = transactionId.getId();
        List keyValues = rowMetadataStore.getCommitTimestamp(table, row);
        if (keyValues != null) {
            int index = 0;
            boolean loop = true;
            while (loop) {
                if (index >= keyValues.size()) {
                    loop = false;
                } else {
                    Object c = keyValues.get(index);
                    long cellTimestamp = dataLib.getKeyValueTimestamp(c);
                    TransactionStruct transaction = transactionStore.getTransactionStatus(new SiTransactionId(cellTimestamp));
                    if (transaction.status.equals(TransactionStatus.COMMITED)) {
                        if (transaction.commitTimestamp > id) {
                            writeWriteConflict(transactionId);
                        }
                    } else if (transaction.status.equals(TransactionStatus.ACTIVE) || transaction.status.equals(TransactionStatus.COMMITTING)) {
                        writeWriteConflict(transactionId);
                    }
                    index++;
                }
            }
        }
    }

    private void writeWriteConflict(TransactionId transactionId) {
        fail(transactionId);
        throw new RuntimeException("write/write conflict");
    }

    @Override
    public Object filterResult(TransactionId transactionId, Object tuple) {
        TransactionStruct transaction = transactionStore.getTransactionStatus(transactionId);
        if (!transaction.status.equals(TransactionStatus.ACTIVE)) {
            throw new RuntimeException("transaction is not ACTIVE");
        }
        List<Object> filteredCells = new ArrayList<Object>();
        final List cells = dataLib.listResult(tuple);
        if (cells != null) {
            for (Object cell : cells) {
                if (shouldKeep(cell, transactionId)) {
                    filteredCells.add(cell);
                }
            }
        }
        return dataLib.newResult(dataLib.getResultKey(tuple), filteredCells);
    }


    public boolean shouldKeep(Object cell, TransactionId transactionId) {
        final long snapshotTimestamp = transactionId.getId();
        final long cellTimestamp = dataLib.getKeyValueTimestamp(cell);
        final TransactionStruct transaction = transactionStore.getTransactionStatus(new SiTransactionId(cellTimestamp));
        switch (transaction.status) {
            case ACTIVE:
                return snapshotTimestamp == cellTimestamp;
            case ERROR:
            case ABORT:
                return false;
            case COMMITTING:
                //TODO: needs special handling
                return false;
            case COMMITED:
                return snapshotTimestamp >= transaction.commitTimestamp;
        }
        throw new RuntimeException("unknown transaction status");
    }

}
