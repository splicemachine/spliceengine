package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.api.FilterState;
import com.splicemachine.si2.api.TransactionId;
import com.splicemachine.si2.api.Transactor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SiFilterTest {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup);
        transactor = transactorSetup.transactor;
    }

    @After
    public void tearDown() throws Exception {
    }

    private void insertAge(TransactionId transactionId, String name, int age) throws IOException {
        SiTransactorTest.insertAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name, age);
    }

    Object readEntireTuple(String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        SGet get = dataLib.newGet(key, null, null, null);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
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
        final TransactionId t1 = transactor.beginTransaction(true, false, false);
        STable table = storeSetup.getReader().open(storeSetup.getPersonTableName());
        final FilterState filterState = transactor.newFilterState(table, t1);
        insertAge(t1, "bill", 20);
        transactor.commit(t1);

        final TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertAge(t2, "bill", 30);

        Object row = readEntireTuple("bill");
        final List keyValues = dataLib.getResultColumn(row, dataLib.encode("_si"), dataLib.encode("commit"));
        for (Object kv : keyValues) {
            transactor.filterKeyValue(filterState, kv);
        }
    }

}
