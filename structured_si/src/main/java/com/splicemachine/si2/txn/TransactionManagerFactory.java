package com.splicemachine.si2.txn;

import com.splicemachine.constants.IHbaseConfigurationSource;
import com.splicemachine.constants.ITransactionManager;
import com.splicemachine.constants.ITransactionManagerFactory;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.TimestampSource;
import com.splicemachine.si2.si.api.Transactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class TransactionManagerFactory implements ITransactionManagerFactory {
    private static ITransactionManager transactionManager;
    private static Transactor transactor;

    @Override
    public void init() {
    }

    @Override
    public ITransactionManager newTransactionManager(IHbaseConfigurationSource configSource) throws IOException {
        if (transactionManager == null) {
            synchronized (TransactionManagerFactory.class) {
                final Configuration configuration = configSource.getConfiguration();
                final Transactor transactor = getTransactor(new IHbaseConfigurationSource() {
                    @Override
                    public Configuration getConfiguration() {
                        return configuration;
                    }
                });
                transactionManager = new TransactionManager(transactor);
            }
        }
        return transactionManager;
    }

    public static void setTransactor(Transactor newTransactor) {
        transactor = newTransactor;
    }

    public static Transactor getTransactor() {
        return getTransactor(new IHbaseConfigurationSource() {
            @Override
            public Configuration getConfiguration() {
                // TODO: this should call SpliceConfiguration
                return HBaseConfiguration.create();
            }
        });
    }

    private static Transactor getTransactor(IHbaseConfigurationSource configSource) {
        if (transactor == null) {
            synchronized (TransactionManagerFactory.class) {
                final Configuration configuration = configSource.getConfiguration();
                TransactionTableCreator.createTransactionTableIfNeeded(configuration);
                TimestampSource timestampSource = new ZooKeeperTimestampSource(TxnConstants.DEFAULT_TRANSACTION_PATH, configuration);
                transactor = TransactorFactory.getTransactor(configuration, timestampSource);
                TransactorFactory.setDefaultTransactor(transactor);
            }
        }
        return transactor;
    }
}
