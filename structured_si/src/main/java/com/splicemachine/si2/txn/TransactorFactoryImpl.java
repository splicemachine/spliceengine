package com.splicemachine.si2.txn;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si2.si.api.HbaseConfigurationSource;
import com.splicemachine.si2.si.api.TimestampSource;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.api.TransactorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class TransactorFactoryImpl implements TransactorFactory {
    private static volatile Transactor transactor;

    @Override
    public void init() {
    }

    @Override
    public Transactor newTransactionManager(HbaseConfigurationSource configSource) throws IOException {
        if (transactor == null) {
            synchronized (TransactorFactoryImpl.class) {
                final Configuration configuration = configSource.getConfiguration();
                transactor = getTransactor(new HbaseConfigurationSource() {
                    @Override
                    public Configuration getConfiguration() {
                        return configuration;
                    }
                });
            }
        }
        return transactor;
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

    private static Transactor getTransactor(HbaseConfigurationSource configSource) {
        if (transactor == null) {
            synchronized (TransactorFactoryImpl.class) {
                final Configuration configuration = configSource.getConfiguration();
                TransactionTableCreator.createTransactionTableIfNeeded(configuration);
                TimestampSource timestampSource = new ZooKeeperTimestampSource(TxnConstants.DEFAULT_TRANSACTION_PATH, configuration);
                transactor = com.splicemachine.si2.data.hbase.TransactorFactory.getTransactor(configuration, timestampSource);
                com.splicemachine.si2.data.hbase.TransactorFactory.setDefaultTransactor(transactor);
            }
        }
        return transactor;
    }
}
