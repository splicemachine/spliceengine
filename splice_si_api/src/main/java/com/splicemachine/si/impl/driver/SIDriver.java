package com.splicemachine.si.impl.driver;

import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.STransactionLib;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.api.driver.SIFactory;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.impl.TimestampServer;

public class SIDriver {

	public static final String SI_FACTORY_CLASS = "com.splicemachine.si.impl.SIFactoryImpl";
	public static final SIFactory siFactory;

	static {
		try {
			siFactory = (SIFactory) Class.forName(SI_FACTORY_CLASS).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static SIFactory getSIFactory() {
		return siFactory;
	}

    public static SDataLib getDataLib() {return siFactory.getDataLib();}

    public static STableWriter getTableWriter() {return siFactory.getTableWriter();}

    public static STableReader getTableReader() {return siFactory.getTableReader();}

    public static DataStore getDataStore() {return siFactory.getDataStore();}

    public static TxnOperationFactory getTxnOperationFactory() {return siFactory.getTxnOperationFactory();}

    public static TxnStore getTxnStore() {return siFactory.getTxnStore();}

    public static TxnSupplier getTxnSupplier() { return siFactory.getTxnSupplier();}

    public static IgnoreTxnCacheSupplier getIgnoreTxnCacheSupplier() {return siFactory.getIgnoreTxnCacheSupplier();}

    public static SOperationStatusLib getOperationStatusLib() {return siFactory.getOperationStatusLib();}

    public static SReturnCodeLib getReturnCodeLib() {return siFactory.getReturnCodeLib();}

    public static TransactionReadController getTransactionReadController() {return siFactory.getTransactionReadController();}

    public static TimestampSource getTimestampSource() {return siFactory.getTimestampSource();}

    public static KeepAliveScheduler getKeepAliveScheduler() {return siFactory.getKeepAliveScheduler();}

    public static SExceptionLib getExceptionLib() {return siFactory.getExceptionLib();}

    public static STransactionLib getTransactionLib() {return siFactory.getTransactionLib();}

    public static TransactionResolver getTransactionResolver() {return siFactory.getTransactionResolver();}

    public static TransactionalRegion getTransactionalRegion(Object region) {
        return siFactory.getTransactionalRegion(region);
    };

    public static TimestampServer getTimestampServer() {
        return siFactory.getTimestampServer();
    }

}
