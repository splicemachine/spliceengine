package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.STableFactory;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.STransactionLib;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.api.driver.SIFactory;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.impl.TimestampServer;

public class SIDriver {

	public static final String SI_FACTORY_CLASS = "com.splicemachine.si.impl.SIFactoryImpl";
	public static final SIFactory siFactory;

    public static STableFactory getTableFactory(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }


    static {
		try {
			siFactory = (SIFactory) Class.forName(SI_FACTORY_CLASS).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    /**
     * @return the configuration specific to this architecture.
     */
    public static SConfiguration getConfiguration(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

	public static SIFactory getSIFactory() {
		return siFactory;
	}

    public static SDataLib getDataLib() {return siFactory.getDataLib();}

    public static DataStore getDataStore() {return siFactory.getDataStore();}

    public static TxnOperationFactory getTxnOperationFactory() {return siFactory.getTxnOperationFactory();}

    /**
     * Get the base Transaction storage mechanism. The implementation returned
     * here is focused on <em>only</em> handling the network layer communication, and should
     * never be called except to set up the more sophisticated stores available in the transaction
     * package (which include caching etc). {@link com.splicemachine.si.impl.TransactionStorage.getTxnStore()}
     * should be used instead.
     *
     * @return a basic Transaction store, specific to this architecture
     */
    public static TxnStore getTxnStore() {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public static TxnSupplier getTxnSupplier() { return siFactory.getTxnSupplier();}

    public static IgnoreTxnCacheSupplier getIgnoreTxnCacheSupplier() {return siFactory.getIgnoreTxnCacheSupplier();}

    public static OperationStatusFactory getOperationStatusLib() {return siFactory.getOperationStatusLib();}

    public static TransactionReadController getTransactionReadController() {return siFactory.getTransactionReadController();}

    public static TimestampSource getTimestampSource() {return siFactory.getTimestampSource();}

    public static KeepAliveScheduler getKeepAliveScheduler() {return siFactory.getKeepAliveScheduler();}

    public static ExceptionFactory getExceptionLib() {return siFactory.getExceptionLib();}

    public static STransactionLib getTransactionLib() {return siFactory.getTransactionLib();}

    public static TransactionalRegion getTransactionalRegion(Object region) {
        return siFactory.getTransactionalRegion(region);
    };

    public static TimestampServer getTimestampServer() {
        return siFactory.getTimestampServer();
    }

}
