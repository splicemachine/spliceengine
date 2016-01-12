package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.impl.TxnPartition;
import com.splicemachine.si.impl.driver.SIDriver;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
@ThreadSafe
public class MTxnPartitionFactory implements PartitionFactory<Object>{
    private final PartitionFactory baseFactory;
    private volatile boolean initialized = false;
    private Transactor transactor;
    private RollForward rollForward;
    private TxnOperationFactory txnOpFactory;
    private TransactionReadController txnReadController;
    private ReadResolver readResolver;


    public MTxnPartitionFactory(PartitionFactory baseFactory){
        this.baseFactory=baseFactory;
    }

    public MTxnPartitionFactory(MPartitionFactory baseFactory,
                                Transactor transactor,
                                RollForward rollForward,
                                TxnOperationFactory txnOpFactory,
                                TransactionReadController txnReadController,
                                ReadResolver readResolver){
        this.baseFactory=baseFactory;
        this.transactor=transactor;
        this.rollForward=rollForward;
        this.txnOpFactory=txnOpFactory;
        this.txnReadController=txnReadController;
        this.readResolver=readResolver;
        this.initialized = true;
    }

    @Override
    public void initialize(Clock clock,SConfiguration configuration) throws IOException{
        baseFactory.initialize(clock,configuration);
    }

    @Override
    public Partition getTable(Object tableName) throws IOException{
        return getTable((String)tableName);
    }


    @Override
    public Partition getTable(String name) throws IOException{
        final Partition delegate=baseFactory.getTable(name);
        if(!initializeIfNeeded(delegate)) return delegate;
        return new TxnPartition(delegate,transactor,rollForward,txnOpFactory,txnReadController,readResolver);
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        final Partition delegate=baseFactory.getTable(name);
        if(!initializeIfNeeded(delegate)) return delegate;
        return new TxnPartition(delegate,transactor,rollForward,txnOpFactory,txnReadController,readResolver);
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return baseFactory.getAdmin();
    }

    private boolean initializeIfNeeded(Partition basePartition){
        if(!initialized){
            synchronized(this){
                if(initialized) return true;
                SIDriver driver = SIDriver.driver();
                if(driver==null) return false;
                transactor = driver.getTransactor();
                rollForward = driver.getRollForward();
                txnOpFactory = driver.getOperationFactory();
                txnReadController = driver.readController();
                readResolver = driver.getReadResolver(basePartition);
                initialized = true;
            }
        }
        return true;
    }
}
