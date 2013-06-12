package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.TransactionId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SIFilterTest extends SIConstants {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup, true);
        transactor = transactorSetup.transactor;
    }

    @After
    public void tearDown() throws Exception {
    }

    private void insertAge(TransactionId transactionId, String name, int age) throws IOException {
        SITransactorTest.insertAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name, age);
    }

    Object readEntireTuple(String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object get = dataLib.newGet(key, null, null, null);
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
        final TransactionId t1 = transactor.beginTransaction();
        STable table = storeSetup.getReader().open(storeSetup.getPersonTableName());
        final FilterState filterState = transactor.newFilterState(transactorSetup.rollForwardQueue, t1, false, false);
        insertAge(t1, "bill", 20);
        transactor.commit(t1);

        final TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "bill", 30);

        Object row = readEntireTuple("bill");
        final List keyValues = dataLib.getResultColumn(row, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY), dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
        for (Object kv : keyValues) {
            transactor.filterKeyValue(filterState, kv);
        }
    }

}
