/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.TxnOperationFactory;
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
    private volatile Transactor transactor;
    private volatile TxnOperationFactory txnOpFactory;

    public MTxnPartitionFactory(PartitionFactory baseFactory){
        this.baseFactory=baseFactory;
    }

    public MTxnPartitionFactory(MPartitionFactory baseFactory,
                                Transactor transactor,
                                TxnOperationFactory txnOpFactory){
        this.baseFactory=baseFactory;
        this.transactor=transactor;
        this.txnOpFactory=txnOpFactory;
        this.initialized = true;
    }

    @Override
    public void initialize(Clock clock,SConfiguration configuration,PartitionInfoCache partitionInfoCache) throws IOException{
        baseFactory.initialize(clock,configuration,partitionInfoCache);
    }

    @Override
    public Partition getTable(Object tableName) throws IOException{
        return getTable((String)tableName);
    }


    @Override
    public Partition getTable(String name) throws IOException{
        final Partition delegate=baseFactory.getTable(name);
        if(!initializeIfNeeded(delegate)) return delegate;
        return wrapPartition(delegate);
    }


    @Override
    public Partition getTable(byte[] name) throws IOException{
        final Partition delegate=baseFactory.getTable(name);
        if(!initializeIfNeeded(delegate)) return delegate;
        return wrapPartition(delegate);
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return baseFactory.getAdmin();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private Partition wrapPartition(Partition delegate){
        return new TxnPartition(delegate,transactor,txnOpFactory);
    }

    private boolean initializeIfNeeded(Partition basePartition){
        if(!initialized){
            synchronized(this){
                if(initialized) return true;
                SIDriver driver = SIDriver.driver();
                if(driver==null) return false;
                transactor = driver.getTransactor();
                txnOpFactory = driver.getOperationFactory();
                initialized = true;
            }
        }
        return true;
    }
}
