package com.splicemachine.si.api;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SITransactionReadController;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.si.jmx.TransactorStatus;
import org.apache.hadoop.hbase.client.*;

/**
 * Used to construct a transactor object that is bound to the HBase types and that provides SI functionality. This is
 * the main entry point for applications using SI. Keeps track of a global, static instance that is shared by all callers.
 * Also defines the various configuration options and plugins for the transactor instance.
 */
public class HTransactorFactory extends SIConstants {

    private static volatile boolean initialized;
    private static volatile ManagedTransactor managedTransactor;
    private static volatile SITransactionReadController< Get, Scan, Delete, Put> readController;

    public static void setTransactor(ManagedTransactor managedTransactorToUse) {
        managedTransactor = managedTransactorToUse;
    }

    public static TransactorStatus getTransactorStatus() {
        initializeIfNeeded();
        return managedTransactor;
    }

    public static Transactor<IHTable, Mutation,Put> getTransactor() {
        initializeIfNeeded();
        return managedTransactor.getTransactor();
    }

    public static TransactionReadController<Get,Scan> getTransactionReadController(){
        initializeIfNeeded();
        return readController;
    }

    @SuppressWarnings("unchecked")
    private static void initializeIfNeeded(){
        if(initialized) return;

        synchronized (HTransactorFactory.class) {
            if(initialized) return;
            if(managedTransactor!=null){
                //it was externally created
                initialized = true;
                return;
            }


            SDataLib dataLib = new HDataLib();
            final STableWriter writer = new HTableWriter();
            ManagedTransactor builderTransactor = new ManagedTransactor();
            DataStore ds = TxnDataStore.getDataStore();
            TxnLifecycleManager tc = TransactionLifecycle.getLifecycleManager();

            if(readController==null)
                readController = new SITransactionReadController<
                        Get,Scan,Delete,Put>(ds,dataLib, TransactionStorage.getTxnSupplier());
            Transactor transactor = new SITransactor.Builder()
                    .dataLib(dataLib)
                    .dataWriter(writer)
                    .dataStore(ds)
                    .txnStore(TransactionStorage.getTxnSupplier())
                    .build();
            builderTransactor.setTransactor(transactor);
            if(managedTransactor==null)
                managedTransactor = builderTransactor;

            initialized = true;
        }
    }

}
