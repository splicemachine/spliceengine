/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Created: 2/2/13 9:47 AM
 */
public class SpliceZooKeeperManager implements Abortable, Closeable{
    private static final Logger LOG=Logger.getLogger(SpliceZooKeeperManager.class);
    protected ZooKeeperWatcher watcher;
    protected RecoverableZooKeeper rzk;
    private volatile boolean isAborted;

    public SpliceZooKeeperManager(){
        initialize(HConfiguration.getConfiguration());
    }

    public void initialize(SConfiguration configuration){
        try{
            watcher=new ZooKeeperWatcher((Configuration) configuration.getConfigSource().unwrapDelegate(), "spliceconnection", this);
            rzk=watcher.getRecoverableZooKeeper();
        }catch(Exception e){
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    public ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException{
        return watcher;
    }

    public RecoverableZooKeeper getRecoverableZooKeeper() throws ZooKeeperConnectionException{
        return rzk;
    }

    @Override
    public void abort(String why,Throwable e){
        if(e instanceof KeeperException.SessionExpiredException){
            try{
                LOG.info("Lost connection with ZooKeeper, attempting reconnect");
                watcher=null;
                initialize(HConfiguration.getConfiguration());
                LOG.info("Successfully reconnected to ZooKeeper");
            }catch(RuntimeException zce){
                LOG.error("Could not reconnect to zookeeper after session expiration, aborting");
                e=zce;
            }
        }
        if(e!=null)
            LOG.error(why,e);
        else
            LOG.error(why);
        this.isAborted=true;
    }

    @Override
    public boolean isAborted(){
        return isAborted;
    }

    @Override
    public void close() throws IOException{
        if(watcher!=null){
            watcher.close();
            watcher=null;
        }
        if(rzk!=null){
            try{
                rzk.close();
                rzk=null;
            }catch(InterruptedException e){
                throw new IOException(e);
            }
        }
    }

    public interface Command<T>{
        T execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException;
    }

    public <T> T executeUnlessExpired(Command<T> command) throws InterruptedException, KeeperException{
        /*
         * What actually happens is that, in the event of a long network partition, ZooKeeper will throw
         * ConnectionLoss exceptions, but it will NOT throw a SessionExpired exception until it reconnects, even
         * if it's been disconnected for CLEARLY longer than the session timeout.
         *
         * To deal with this, we have to basically loop through our command repeatedly until we either
         *
         * 1. Succeed.
         * 2. Get a SessionExpired event from ZooKeeper
         * 3. Spent more than 2*sessionTimeout ms attempting the request
         * 4. Get some other kind of Zk error (NoNode, etc).
         */
        RecoverableZooKeeper rzk;
        try{
            rzk=getRecoverableZooKeeper();
        }catch(ZooKeeperConnectionException e){
            throw new KeeperException.SessionExpiredException();
        }
        //multiple by 2 to make absolutely certain we're timed out.
        int sessionTimeout=2*rzk.getZooKeeper().getSessionTimeout();
        long nextTime=System.currentTimeMillis();
        long startTime=System.currentTimeMillis();
        while((int)(nextTime-startTime)<sessionTimeout){
            try{
                return command.execute(rzk);
            }catch(KeeperException ke){
                switch(ke.code()){
                    case CONNECTIONLOSS:
                    case OPERATIONTIMEOUT:
                        LOG.warn("Detected a Connection issue("+ke.code()+") with ZooKeeper, retrying");
                        nextTime=System.currentTimeMillis();
                        break;
                    default:
                        throw ke;
                }
            }
        }

        //we've run out of time, our session has almost certainly expired. Give up and explode
        throw new KeeperException.SessionExpiredException();
    }

    public <T> T execute(Command<T> command) throws InterruptedException, KeeperException{
        try{
            return command.execute(getRecoverableZooKeeper());
        }catch(ZooKeeperConnectionException e){
            throw new KeeperException.SessionExpiredException();
        }
    }
}
