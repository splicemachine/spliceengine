package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
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
    private final Object siMetaQualifier;
    private final Object encodedSiMetaQualifier;
    private final Object siLockQualifier;
    private final Object encodedSiLockQualifier;
    private final Object siMetaNull;
    private final Object encodedSiMetaNull;

    public SiTransactor(IdSource idSource, TupleHandler dataTupleHandler, RelationReader dataReader, RelationWriter dataWriter,
                        TransactionStore transactionStore,
                        String siNeededAttributeName, String siMetaFamily, Object siMetaQualifier, Object siLockQualifier,
                        Object siMetaNull) {
        this.idSource = idSource;
        this.dataTupleHandler = dataTupleHandler;
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
        this.transactionStore = transactionStore;
        this.siNeededAttributeName = siNeededAttributeName;
        this.siMetaFamily = siMetaFamily;
        this.encodedSiMetaFamily = dataTupleHandler.makeFamily(siMetaFamily);
        this.siMetaQualifier = siMetaQualifier;
        this.encodedSiMetaQualifier = dataTupleHandler.makeQualifier(siMetaQualifier);
        this.siLockQualifier = siLockQualifier;
        this.encodedSiLockQualifier = dataTupleHandler.makeQualifier(siLockQualifier);
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
        if(!transactionStatus[0].equals(TransactionStatus.ACTIVE)) {
            throw new RuntimeException( "transaction is not ACTIVE");
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
                TuplePut newPut = dataTupleHandler.makeTuplePut(dataTupleHandler.getKey(t), null);
                for (Object cell : dataTupleHandler.getCells(t)) {
                    dataTupleHandler.addCellToTuple(newPut, dataTupleHandler.getCellFamily(cell),
                            dataTupleHandler.getCellQualifier(cell),
                            siTransactionId.id,
                            dataTupleHandler.getCellValue(cell));
                }
                dataTupleHandler.addCellToTuple(newPut, encodedSiMetaFamily, encodedSiMetaQualifier, siTransactionId.id, encodedSiMetaNull);
                results.add(newPut);
                obtainWriteLock(transactionId, relation, t);
            } else {
                results.add(t);
            }
        }
        return results;
    }

    private void obtainWriteLock(TransactionId transactionId, Relation relation, TuplePut t) {
        Object key = dataTupleHandler.getKey(t);
        TuplePut put = dataTupleHandler.makeTuplePut(key, null);
        Object idValue = dataTupleHandler.makeValue(((SiTransactionId) transactionId).id);
        dataTupleHandler.addCellToTuple(put, encodedSiMetaFamily, encodedSiLockQualifier, null, idValue);
        if (!dataWriter.checkAndPut(relation, encodedSiMetaFamily, encodedSiLockQualifier, null, put)) {
            List columns = Arrays.asList(Arrays.asList(encodedSiMetaFamily, encodedSiLockQualifier));
            TupleGet get = dataTupleHandler.makeTupleGet(key, key, null, columns, null);
            Iterator result = dataReader.read(relation, get);
            Object lockValue = null;
            if (result.hasNext()) {
                lockValue = dataTupleHandler.getLatestCellForColumn(result.next(), encodedSiMetaFamily, encodedSiLockQualifier);
                Long decodedLockValue = (Long) dataTupleHandler.fromValue(lockValue, Long.class);
                Object[] transactionStatus = transactionStore.getTransactionStatus(new SiTransactionId(decodedLockValue));
                if (((TransactionStatus) transactionStatus[0]).equals(TransactionStatus.ACTIVE)) {
                    writeWriteConflict(transactionId);
                }
            }
            if(!dataWriter.checkAndPut(relation, encodedSiMetaFamily, encodedSiLockQualifier, lockValue, put)) {
                writeWriteConflict(transactionId);
            }
        }
    }

    private void writeWriteConflict(TransactionId transactionId) {
        failTransaction(transactionId);
        throw new RuntimeException("write/write conflict");
    }

    @Override
    public Object filterTuple(TransactionId transactionId, Object tuple) {
        List<Object> filteredCells = new ArrayList<Object>();
        for (Object cell : dataTupleHandler.getCells(tuple)) {
            if (shouldKeep(cell, transactionId)) {
                filteredCells.add(cell);
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
