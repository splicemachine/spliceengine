package com.splicemachine.si.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.HbaseConfigurationSource;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TransactorFactory;
import com.splicemachine.si.txn.TransactionTableCreator;
import com.splicemachine.si.txn.ZooKeeperTimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class TransactorFactoryImpl implements TransactorFactory {
    private static volatile Transactor transactor;

    @Override
    public Transactor newTransactor(HbaseConfigurationSource configSource) throws IOException {
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
                TimestampSource timestampSource = new ZooKeeperTimestampSource(SpliceConstants.DEFAULT_TRANSACTION_PATH, configuration);
                transactor = com.splicemachine.si.data.hbase.TransactorFactory.getTransactor(configuration, timestampSource);
                com.splicemachine.si.data.hbase.TransactorFactory.setDefaultTransactor(transactor);
            }
        }
        return transactor;
    }
}
