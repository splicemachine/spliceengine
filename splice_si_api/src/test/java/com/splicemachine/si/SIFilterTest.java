package com.splicemachine.si;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.si.testenv.TransactorTestUtility;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.Partition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("unchecked")
public class SIFilterTest{
    boolean useSimple=true;

    SITestEnv testEnv;
    TestTransactionSetup transactorSetup;
    Transactor transactor;
    TxnLifecycleManager control;
    TransactionReadController readController;

    @Before
    public void setUp() throws Exception{
        if(testEnv==null)
            testEnv = SITestEnvironment.storeSetup();
        transactorSetup=new TestTransactionSetup(testEnv,true);
        baseSetup();
    }

    protected void baseSetup(){
        transactor=transactorSetup.transactor;
        control=transactorSetup.txnLifecycleManager;
        readController=transactorSetup.readController;
    }

    @After
    public void tearDown() throws Exception{
    }

    private void insertAge(Txn txn,String name,int age) throws IOException{
        TransactorTestUtility.insertAgeDirect(useSimple,transactorSetup,testEnv,txn,name,age);
    }

    DataResult readEntireTuple(String name) throws IOException{
        final SDataLib dataLib=testEnv.getDataLib();

        byte[] key=dataLib.newRowKey(new Object[]{name});
        Object get=dataLib.newGet(key,null,null,null);
        try(Partition table=transactorSetup.getPersonTable(testEnv)){
            return (DataResult)table.get(get);
        }
    }

    @Test
    public void testFiltering() throws Exception{
        final SDataLib dataLib=testEnv.getDataLib();
        final Txn t1=control.beginTransaction(Bytes.toBytes("1184"));
        final TxnFilter filterState=readController.newFilterState(transactorSetup.readResolver,t1);
        insertAge(t1,"bill",20);
        t1.commit();

        final Txn t2=control.beginTransaction(Bytes.toBytes("1184"));
        insertAge(t2,"bill",30);

        DataResult row=readEntireTuple("bill");
        final Iterable<DataCell> keyValues=row.columnCells(dataLib.encode(SIConstants.DEFAULT_FAMILY_BYTES),SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
        for(DataCell kv : keyValues){
            filterState.filterKeyValue(kv);
        }
    }

}
