package com.splicemachine.si;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;

@SuppressWarnings("unchecked")
public class SIFilterTest extends SIConstants {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TestTransactionSetup transactorSetup;
    Transactor transactor;
		TransactionManager control;
		TransactionReadController readController;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
				baseSetup();
    }

		protected void baseSetup() {
				transactor = transactorSetup.transactor;
				control = transactorSetup.control;
				readController = transactorSetup.readController;
		}

		@After
    public void tearDown() throws Exception {
    }

    private void insertAge(TransactionId transactionId, String name, int age) throws IOException {
        TransactorTestUtility.insertAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name, age);
    }

    Result readEntireTuple(String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        Object get = dataLib.newGet(key, null, null, null);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            return reader.get(testSTable, get);
        } finally {
            reader.close(testSTable);
        }
    }

    @Test
    public void testFiltering() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final TransactionId t1 = control.beginTransaction();
        final IFilterState filterState = readController.newFilterState(transactorSetup.rollForwardQueue, t1);
        insertAge(t1, "bill", 20);
        control.commit(t1);

        final TransactionId t2 = control.beginTransaction();
        insertAge(t2, "bill", 30);

        Result row = readEntireTuple("bill");
        final List<Cell> keyValues = row.getColumnCells(dataLib.encode(DEFAULT_FAMILY_BYTES), dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
        for (Cell kv : keyValues) {
						filterState.filterCell(kv);
        }
    }

}
