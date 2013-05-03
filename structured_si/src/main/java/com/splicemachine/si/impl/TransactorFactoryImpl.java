package com.splicemachine.si.impl;

import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.si.api.HbaseConfigurationSource;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TransactorFactory;
import com.splicemachine.si.txn.TransactionTableCreator;
import com.splicemachine.si.txn.ZooKeeperTimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Source of transactors. If code needs a transactor and wasn't handed one, then it can use the static methods here to
 * obtain "the" transactor.
 */
public class TransactorFactoryImpl implements TransactorFactory {
    private static volatile Transactor transactor;
    //deliberate boxing here to ensure the lock is not shared by anyone else
    private static final Object lock = new Integer(1);

    @Override
    public Transactor newTransactor(HbaseConfigurationSource configSource) throws IOException {
        return getTransactor(configSource);
    }

    public static void setTransactor(Transactor newTransactor) {
        transactor = newTransactor;
    }

    public static Transactor getTransactor() {
        return getTransactor(new HbaseConfigurationSource() {
            @Override
            public Configuration getConfiguration() {
                // TODO: this should call SpliceConfiguration
                return HBaseConfiguration.create();
            }
        });
    }

    public static Transactor getTransactor(final Configuration conf){
        return getTransactor(new HbaseConfigurationSource() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
        });
    }

    public static Transactor getTransactor(HbaseConfigurationSource configSource) {
        if(transactor!=null) return transactor;
        synchronized (lock) {
            //double-checked locking--make sure someone else didn't already create it
            if(transactor!=null)
                return transactor;

            final Configuration configuration = configSource.getConfiguration();
//            TransactionTableCreator.createTransactionTableIfNeeded(configuration);
            TimestampSource timestampSource = new ZooKeeperTimestampSource(TransactionConstants.DEFAULT_TRANSACTION_PATH, configuration);
            transactor = com.splicemachine.si.data.hbase.TransactorFactory.getTransactor(configuration, timestampSource);
            com.splicemachine.si.data.hbase.TransactorFactory.setDefaultTransactor(transactor);
        }
        return transactor;
    }
}
