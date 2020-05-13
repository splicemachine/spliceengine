/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.lifecycle;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class MasterLifecycle implements DistributedDerbyStartup{
    private boolean loading = false;
    @Override
    public void distributedStart() throws IOException{
        try{
            HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(SIDriver.driver().getConfiguration());
            if(!ZkUtils.isSpliceLoaded()){
                loading = true;
                instance.createSpliceHBaseTables();
            }
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }catch(KeeperException e){
            throw new IOException(e);
        }
    }

    @Override
    public void markBootFinished() throws IOException{
        if(loading){
            try{
                ZkUtils.spliceFinishedLoading();
            }catch(InterruptedException e){
                throw new InterruptedIOException();
            }catch(KeeperException e){
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean connectAsFirstTime(){
        return loading;
    }
}
