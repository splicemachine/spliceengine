package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SiFilterTest {
    final boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        if (!useSimple) {
            storeSetup = new HStoreSetup();
        }
        transactorSetup = new TransactorSetup(storeSetup);
        transactor = transactorSetup.transactor;
    }

    private void insertAge(TransactionId transactionId, String name, int age) {
        SiTransactorTest.insertAgeDirect(transactorSetup, storeSetup, transactionId, name, age);
    }

    private String read(TransactionId transactionId, String name) {
        return SiTransactorTest.readAgeDirect(transactorSetup, storeSetup, transactionId, name);
    }

    Object readEntireTuple(String name) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        SGet get = dataLib.newGet(key, null, null, null);
        STable testSTable = reader.open("people");
        try {
            return reader.get(testSTable, get);
        } finally {
            reader.close(testSTable);
        }
    }

    @Test
    public void testFiltering() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final Transactor transactor = transactorSetup.transactor;
        final TransactionId t1 = transactor.beginTransaction();
        STable table = storeSetup.getReader().open("people");
        final FilterState filterState = transactor.newFilterState(table, t1);
        insertAge(t1, "joe", 20);
        transactor.commit(t1);

        final TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe", 30);

        Object row = readEntireTuple("joe");
        final List keyValues = dataLib.getResultColumn(row, dataLib.encode("_si"), dataLib.encode("commit"));
        for (Object kv : keyValues) {
            transactor.filterKeyValue(filterState, kv);
        }
    }

}
