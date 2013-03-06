package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.si.api.TransactionId;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RowMetadataStore {
    private final SDataLib dataLib;
    private final STableReader reader;

    private final String siNeededAttribute;

    private final Object siFamily;
    private final Object commitTimestampQualifier;
    private final Object siNull;

    public RowMetadataStore(SDataLib dataLib, STableReader reader, String siNeededAttribute,
                            String siMetaFamily, Object siCommitQualifier, Object siMetaNull) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.siNeededAttribute = siNeededAttribute;
        this.siFamily = dataLib.encode(siMetaFamily);
        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.siNull = dataLib.encode(siMetaNull);
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

    Object clonePut(TransactionId transactionId, Object put, Object rowKey, SRowLock lock) {
        Object newPut = dataLib.newPut(rowKey, lock);
        for (Object cell : dataLib.listPut(put)) {
            dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(cell),
                    dataLib.getKeyValueQualifier(cell),
                    transactionId.getId(),
                    dataLib.getKeyValueValue(cell));
        }
        return newPut;
    }

    List getCommitTimestamp(STable table, Object row) {
        final List<List<Object>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        SGet get = dataLib.newGet(row, null, columns, null);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultColumn(result, siFamily, commitTimestampQualifier);
        }
        return null;
    }

}
