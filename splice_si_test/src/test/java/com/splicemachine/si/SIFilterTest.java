package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.impl.TxnFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("unchecked")
public class SIFilterTest extends SIConstants {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TestTransactionSetup transactorSetup;
    Transactor transactor;
		TxnLifecycleManager control;
		TransactionReadController readController;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
				baseSetup();
    }

		protected void baseSetup() {
				transactor = transactorSetup.transactor;
				control = transactorSetup.txnLifecycleManager;
				readController = transactorSetup.readController;
		}

		@After
    public void tearDown() throws Exception {
    }

    private void insertAge(Txn txn, String name, int age) throws IOException {
        TransactorTestUtility.insertAgeDirect(useSimple, transactorSetup, storeSetup, txn, name, age);
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
        final Txn t1 = control.beginTransaction(Bytes.toBytes("1184"));
        final TxnFilter filterState = readController.newFilterState(transactorSetup.readResolver, t1);
        insertAge(t1, "bill", 20);
				t1.commit();

        final Txn t2 = control.beginTransaction(Bytes.toBytes("1184"));
        insertAge(t2, "bill", 30);

        Result row = readEntireTuple("bill");
        final List<KeyValue> keyValues = row.getColumn(dataLib.encode(DEFAULT_FAMILY_BYTES), SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
        for (KeyValue kv : keyValues) {
						filterState.filterKeyValue(kv);
        }
    }

}
