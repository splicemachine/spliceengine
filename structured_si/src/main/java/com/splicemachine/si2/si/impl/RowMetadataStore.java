package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.TransactionId;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RowMetadataStore {
    private final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    private final String siNeededAttribute;
    private final String transactionIdAttribute;

    private final Object siFamily;
    private final Object commitTimestampQualifier;
    private final Object siNull;

    private final Object userColumnFamily;

    public RowMetadataStore(SDataLib dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                            String transactionIdAttribute,
                            String siMetaFamily, Object siCommitQualifier, Object siMetaNull,
                            Object userColumnFamily) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.transactionIdAttribute = transactionIdAttribute;
        this.siFamily = dataLib.encode(siMetaFamily);
        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.siNull = dataLib.encode(siMetaNull);
        this.userColumnFamily = dataLib.encode(userColumnFamily);
    }

    void setSiNeededAttribute(Object put) {
        dataLib.addAttribute(put, siNeededAttribute, dataLib.encode(true));
    }

    Boolean getSiNeededAttribute(Object put) {
        Object neededValue = dataLib.getAttribute(put, siNeededAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPut(Object put, TransactionId transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId.getId(), siNull);
    }

    void setTransactionId(SiTransactionId transactionId, Object put) {
        dataLib.addAttribute(put, transactionIdAttribute, dataLib.encode(transactionId.getId()));
    }

    SiTransactionId getTransactionIdFromPut(Object put) {
        Object value = dataLib.getAttribute(put, transactionIdAttribute);
        Long transactionId = (Long) dataLib.decode(value, Long.class);
        if (transactionId != null) {
            return new SiTransactionId(transactionId);
        }
        return null;
    }

    Object newLockWithPut(TransactionId transactionId, Object put, SRowLock lock) {
        Object rowKey = dataLib.getPutKey(put);
        Object newPut = dataLib.newPut(rowKey, lock);
        for (Object keyValue : dataLib.listPut(put)) {
            dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                    dataLib.getKeyValueQualifier(keyValue),
                    transactionId.getId(),
                    dataLib.getKeyValueValue(keyValue));
        }
        return newPut;
    }

    List getCommitTimestamp(STable table, Object rowKey) {
        final List<List<Object>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        SGet get = dataLib.newGet(rowKey, null, columns, null);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultColumn(result, siFamily, commitTimestampQualifier);
        }
        return null;
    }

    public boolean isCommitTimestampKeyValue(Object keyValue) {
        return dataLib.valuesEqual(dataLib.getKeyValueFamily(keyValue), siFamily) &&
                dataLib.valuesEqual(dataLib.getKeyValueQualifier(keyValue), commitTimestampQualifier);
    }

    public boolean isSiNull(Object value) {
        return dataLib.valuesEqual(value, siNull);
    }

    public boolean isDataKeyValue(Object keyValue) {
        return dataLib.valuesEqual(dataLib.getKeyValueFamily(keyValue), userColumnFamily);
    }

    public void setCommitTimestamp(STable table, Object keyValue, long beginTimestamp, long commitTimestamp) {
        Object put = dataLib.newPut(dataLib.getKeyValueRow(keyValue));
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, beginTimestamp, dataLib.encode(commitTimestamp));
        writer.write(table, put, false);
    }
}
