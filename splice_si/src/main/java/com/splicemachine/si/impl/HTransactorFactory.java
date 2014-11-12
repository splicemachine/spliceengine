package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.si.jmx.TransactorStatus;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

/**
 * Used to construct a transactor object that is bound to the HBase types and that provides SI functionality. This is
 * the main entry point for applications using SI. Keeps track of a global, static instance that is shared by all callers.
 * Also defines the various configuration options and plugins for the transactor instance.
 */
public class HTransactorFactory extends SIConstants {

    private static volatile boolean initialized;
    private static volatile ManagedTransactor managedTransactor;
    private static volatile SITransactionReadController<KeyValue,Get, Scan, Delete, Put> readController;

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

    public static TransactionReadController<KeyValue,Get,Scan> getTransactionReadController(){
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


            SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
            final STableWriter writer = SIFactoryDriver.siFactory.getTableWriter();
            ManagedTransactor builderTransactor = new ManagedTransactor();
            DataStore ds = TxnDataStore.getDataStore();
            TxnLifecycleManager tc = TransactionLifecycle.getLifecycleManager();

            if(readController==null)
                readController = new SITransactionReadController<KeyValue,
                        Get,Scan,Delete,Put>(ds,dataLib, SIFactoryDriver.siFactory.getTxnSupplier());
            Transactor transactor = new SITransactor.Builder()
                    .dataLib(dataLib)
                    .dataWriter(writer)
                    .dataStore(ds)
                    .txnStore(SIFactoryDriver.siFactory.getTxnSupplier())
                    .build();
            builderTransactor.setTransactor(transactor);
            if(managedTransactor==null)
                managedTransactor = builderTransactor;

            initialized = true;
        }
    }

}
