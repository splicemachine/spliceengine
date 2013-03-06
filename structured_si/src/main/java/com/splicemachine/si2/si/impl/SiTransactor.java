package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.RowLock;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.api.TuplePut;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.IdSource;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SiTransactor implements Transactor, ClientTransactor {
    private final IdSource idSource;
    private final TupleHandler dataTupleHandler;
    private final RelationReader dataReader;
    private final RelationWriter dataWriter;
    private final TransactionStore transactionStore;
    private final String siNeededAttributeName;
    private final String siMetaFamily;
    private final Object encodedSiMetaFamily;
    private final Object siCommitQualifier;
    private final Object encodedSiCommitQualifier;
    private final Object siMetaNull;
    private final Object encodedSiMetaNull;

    public SiTransactor(IdSource idSource, TupleHandler dataTupleHandler, RelationReader dataReader, RelationWriter dataWriter,
                        TransactionStore transactionStore,
                        String siNeededAttributeName, String siMetaFamily, Object siCommitQualifier, Object siLockQualifier,
                        Object siMetaNull) {
        this.idSource = idSource;
        this.dataTupleHandler = dataTupleHandler;
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
        this.transactionStore = transactionStore;
        this.siNeededAttributeName = siNeededAttributeName;
        this.siMetaFamily = siMetaFamily;
        this.encodedSiMetaFamily = dataTupleHandler.makeFamily(siMetaFamily);
        this.siCommitQualifier = siCommitQualifier;
        this.encodedSiCommitQualifier = dataTupleHandler.makeQualifier(siCommitQualifier);
        this.siMetaNull = siMetaNull;
        this.encodedSiMetaNull = dataTupleHandler.makeValue(siMetaNull);
    }

    @Override
    public TransactionId beginTransaction() {
        final SiTransactionId transactionId = new SiTransactionId(idSource.nextId());
        transactionStore.recordNewTransaction(transactionId, TransactionStatus.ACTIVE);
        return transactionId;
    }

    @Override
    public void commitTransaction(TransactionId transactionId) {
        Object[] transactionStatus = transactionStore.getTransactionStatus((SiTransactionId) transactionId);
        if (!transactionStatus[0].equals(TransactionStatus.ACTIVE)) {
            throw new RuntimeException("transaction is not ACTIVE");
        }
        transactionStore.recordTransactionStatusChange((SiTransactionId) transactionId, TransactionStatus.COMMITTING);
        final long endId = idSource.nextId();
        transactionStore.recordTransactionCommit((SiTransactionId) transactionId, endId, TransactionStatus.COMMITED);
    }

    @Override
    public void abortTransaction(TransactionId transactionId) {
        transactionStore.recordTransactionStatusChange((SiTransactionId) transactionId, TransactionStatus.ABORT);
    }

    @Override
    public void failTransaction(TransactionId transactionId) {
        transactionStore.recordTransactionStatusChange((SiTransactionId) transactionId, TransactionStatus.ERROR);
    }

    @Override
    public void initializeTuplePuts(List<TuplePut> tuples) {
        for (Object t : tuples) {
            dataTupleHandler.addAttributeToTuple(t, siNeededAttributeName, dataTupleHandler.makeValue(true));
        }
    }

    @Override
    public List<TuplePut> processTuplePuts(TransactionId transactionId, Relation relation, List<TuplePut> tuples) {
        List<TuplePut> results = new ArrayList<TuplePut>();
        SiTransactionId siTransactionId = (SiTransactionId) transactionId;
        for (TuplePut t : tuples) {
            Object neededValue = dataTupleHandler.getAttribute(t, siNeededAttributeName);
            Boolean siNeeded = (Boolean) dataTupleHandler.fromValue(neededValue, Boolean.class);
            if (siNeeded) {
                Object row = dataTupleHandler.getKey(t);
                RowLock lock = dataWriter.lockRow(relation, row);
                try {
                    checkForConflict(transactionId, relation, lock, row);
                    TuplePut newPut = dataTupleHandler.makeTuplePut(row, lock, null);
                    for (Object cell : dataTupleHandler.getCells(t)) {
                        dataTupleHandler.addCellToTuple(newPut, dataTupleHandler.getCellFamily(cell),
                                dataTupleHandler.getCellQualifier(cell),
                                siTransactionId.id,
                                dataTupleHandler.getCellValue(cell));
                    }
                    dataTupleHandler.addCellToTuple(newPut, encodedSiMetaFamily, encodedSiCommitQualifier, siTransactionId.id, encodedSiMetaNull);
                    dataWriter.write(relation, Arrays.asList(newPut));
                } finally {
                    dataWriter.unLockRow(relation, lock);
                }
            } else {
                results.add(t);
            }
        }
        return results;
    }

    private void checkForConflict(TransactionId transactionId, Relation relation, RowLock lock, Object row) {
        long id = ((SiTransactionId) transactionId).id;
        Object idValue = dataTupleHandler.makeValue(((SiTransactionId) transactionId).id);
        TupleGet get = dataTupleHandler.makeTupleGet(row, row, null, Arrays.asList(Arrays.asList(encodedSiMetaFamily, encodedSiCommitQualifier)), null);
        Iterator results = dataReader.read(relation, get);
        if (results.hasNext()) {
            Object tuple = results.next();
            List cells = dataTupleHandler.getCellsForColumn(tuple, encodedSiMetaFamily, encodedSiCommitQualifier);
            int index = 0;
            boolean loop = true;
            while (loop) {
                if (index >= cells.size()) {
                    loop = false;
                } else {
                    Object c = cells.get(index);
                    long cellTimestamp = dataTupleHandler.getCellTimestamp(c);
                    Object[] transactionStatus = transactionStore.getTransactionStatus(new SiTransactionId(cellTimestamp));
                    TransactionStatus status = (TransactionStatus) transactionStatus[0];
                    Long commitTimestamp = (Long) transactionStatus[1];
                    if (status.equals(TransactionStatus.COMMITED)) {
                        if (commitTimestamp > id) {
                            writeWriteConflict(transactionId);
                        }
                    } else if (status.equals(TransactionStatus.ACTIVE) || status.equals(TransactionStatus.COMMITTING)) {
                        writeWriteConflict(transactionId);
                    }
                    index++;
                }
            }
        }
        assert !results.hasNext();
    }

    private void writeWriteConflict(TransactionId transactionId) {
        failTransaction(transactionId);
        throw new RuntimeException("write/write conflict");
    }

    @Override
    public Object filterTuple(TransactionId transactionId, Object tuple) {
        Object[] transactionStatus = transactionStore.getTransactionStatus((SiTransactionId) transactionId);
        TransactionStatus status = (TransactionStatus) transactionStatus[0];
        if (!status.equals(TransactionStatus.ACTIVE)) {
            throw new RuntimeException("transaction is not ACTIVE");
        }
        List<Object> filteredCells = new ArrayList<Object>();
        final List cells = dataTupleHandler.getCells(tuple);
        if (cells != null) {
            for (Object cell : cells) {
                if (shouldKeep(cell, transactionId)) {
                    filteredCells.add(cell);
                }
            }
        }
        return dataTupleHandler.makeTuple(dataTupleHandler.getKey(tuple), filteredCells);
    }


    public boolean shouldKeep(Object cell, TransactionId transactionId) {
        final long snapshotTimestamp = ((SiTransactionId) transactionId).id;
        final long cellTimestamp = dataTupleHandler.getCellTimestamp(cell);
        final Object[] s = transactionStore.getTransactionStatus(new SiTransactionId(cellTimestamp));
        TransactionStatus transactionStatus = (TransactionStatus) s[0];
        Long commitTimestamp = (Long) s[1];
        switch (transactionStatus) {
            case ACTIVE:
                return snapshotTimestamp == cellTimestamp;
            case ERROR:
            case ABORT:
                return false;
            case COMMITTING:
                //TODO: needs special handling
                return false;
            case COMMITED:
                return snapshotTimestamp >= commitTimestamp;
        }
        throw new RuntimeException("unknown transaction status");
    }

}
