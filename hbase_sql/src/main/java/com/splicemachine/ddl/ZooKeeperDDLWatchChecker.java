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

package com.splicemachine.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import com.splicemachine.access.configuration.DDLConfiguration;
import com.splicemachine.derby.ddl.CommunicationListener;
import com.splicemachine.derby.ddl.DDLWatchChecker;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.Pair;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public class ZooKeeperDDLWatchChecker implements DDLWatchChecker{
    private static final Logger LOG=Logger.getLogger(ZooKeeperDDLWatchChecker.class);
    private String id;
    private Watcher changeIdWatcher;
    private final DDLZookeeperClient zkClient;

    public ZooKeeperDDLWatchChecker(DDLZookeeperClient zkClient){
        this.zkClient=zkClient;
    }

    @Override
    public boolean initialize(final CommunicationListener changeIdListener) throws IOException{

        this.id = zkClient.registerThisServer();
        if(id==null) return false; //not a server, so inform the world
        if(id.startsWith("/"))
            id = id.substring(1); //strip the leading / to make sure that we register properly
        changeIdWatcher=new Watcher(){
            @Override
            public void process(WatchedEvent watchedEvent){
                if(watchedEvent.getType().equals(Event.EventType.NodeChildrenChanged)){
                    if(LOG.isTraceEnabled())
                        LOG.trace("Received watch event, signalling refresh");
                    changeIdListener.onCommunicationEvent(watchedEvent.getPath());
                }
            }
        };
        initZKDemarcationPoint();
        return true;
    }

    private void initZKDemarcationPoint() throws IOException{
        String rootPath = SIDriver.driver().getConfiguration().getSpliceRootPath();
        String ddlPath = rootPath+HConfiguration.DDL_PATH;
        List<String> children = ZkUtils.getChildren(ddlPath, null);
        long max = 0;
        for (String child : children) {
            String demarcationPath = ddlPath + "/" + child;
            byte[] data = ZkUtils.getData(demarcationPath);
            long demarcationPoint = data != null ? Bytes.toLong(data) : 0;
            if (demarcationPoint > max) {
                max = demarcationPoint;
            }
        }
        String demarcationPointPath = ddlPath+"/"+id;
        try {
            ZkUtils.recursiveSafeCreate(demarcationPointPath, Bytes.toBytes(max), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            SpliceLogUtils.info(LOG, "Initialize DDL demarcation point to %d", max);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    @Override
    public Collection<String> getCurrentChangeIds() throws IOException{
        try{
            return ZkUtils.getRecoverableZooKeeper().getChildren(zkClient.changePath,changeIdWatcher);
        }catch(KeeperException ke){
            if(ke.code().equals(KeeperException.Code.SESSIONEXPIRED)){
                LOG.error("ZooKeeper Session expired, terminating early");
                return null;
            }
            throw new IOException(ke);
        }catch(InterruptedException e){
            throw Exceptions.getIOException(e);
        }
    }

    @Override
    public DDLMessage.DDLChange getChange(String changeId) throws IOException{
        byte[] data;
        try {
            data = ZkUtils.getData(zkClient.changePath + "/" + changeId);
        } catch (IOException e) {
            if (e.getCause() instanceof KeeperException.NoNodeException) {
                // the node doesn't exit, return null
                LOG.warn(String.format("Cannot find a node for change with id %s", changeId));
                return null;
            }
            LOG.error(String.format("Cannot get a node for change with id %s", changeId), e);
            throw e;
        }
        return DDLMessage.DDLChange.newBuilder()
                .mergeFrom(DDLMessage.DDLChange.parseFrom(data))
                .setChangeId(changeId).build();
    }

    @Override
    @SuppressFBWarnings(value = "DM_DEFAULT_ENCODING", justification = "DB-10605")
    public void notifyProcessed(Collection<Pair<DDLMessage.DDLChange,String>> processedChanges) throws IOException{
        /*
         * Notify the relevant controllers that their change has been processed
         */
        List<Op> ops = Lists.newArrayListWithExpectedSize(processedChanges.size());
        List<DDLMessage.DDLChange> changeList = Lists.newArrayList();
        for (Pair<DDLMessage.DDLChange,String> pair : processedChanges) {
            DDLMessage.DDLChange change = pair.getFirst();
            String errorMessage = pair.getSecond();
            // Tag Errors for handling on the client, will allow us to understand what node failed and why...
            String path=zkClient.changePath+"/"+change.getChangeId()+"/"+(errorMessage==null?"": DDLConfiguration.ERROR_TAG)+id;
            Op op = Op.create(path, (errorMessage==null?new byte[]{}:(String.format("server [%s] failed with error [%s]",id,errorMessage).getBytes())),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
            ops.add(op);
            changeList.add(change);
        }
        try {
            List<OpResult> multi = ZkUtils.getRecoverableZooKeeper().getZooKeeper().multi(ops);
            for(int i=0;i<multi.size();i++){
                OpResult result = multi.get(i);
                if(!(result instanceof OpResult.ErrorResult))
                    processedChanges.remove(changeList.get(i));
                else{
                    OpResult.ErrorResult err = (OpResult.ErrorResult)result;
                    KeeperException.Code code = KeeperException.Code.get(err.getErr());
                    String errorMessage = String.format("Cannot create a node for change with id %s, code %s", changeList.get(i).getChangeId(), code.toString());
                    switch(code){
                        case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                        case NONODE: // someone already removed the notification, it's obsolete
                            // ignore
                            LOG.warn(errorMessage);
                            break;
                        default:
                            LOG.error(errorMessage);
                            throw Exceptions.getIOException(KeeperException.create(code));
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Exceptions.getIOException(e);
        } catch (KeeperException e) {
            String changeIds = String.join(",", changeList.stream().map(c -> c.getChangeId()).collect(Collectors.toList()));
            String errorMessage = String.format("Could not execute multiple ZooKeeper operations for changes with ids %s, code %s", changeIds, e.code().toString());
            switch(e.code()) {
                case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                case NONODE: // someone already removed the notification, it's obsolete
                    LOG.warn(errorMessage);
                    // ignore
                    break;
                default:
                    LOG.error(errorMessage, e);
                    throw Exceptions.getIOException(e);
            }
        }
    }

    @Override
    public void killDDLTransaction(String key){
       zkClient.deleteChangeNode(key);
    }


    @Override
    public void assignDDLDemarcationPoint(long txnId) throws Exception {
        String rootPath = SIDriver.driver().getConfiguration().getSpliceRootPath();
        String demarcationPath = rootPath+HConfiguration.DDL_PATH+"/"+id;
        byte[] data = Bytes.toBytes(txnId);
        ZkUtils.setData(demarcationPath, data, -1);
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "Set DDL demarcation point to %d", txnId);
        }

    }

    @Override
    public long initDemarcationPoint() throws IOException{
        String rootPath = SIDriver.driver().getConfiguration().getSpliceRootPath();
        String demarcationPointPath = rootPath+HConfiguration.DDL_PATH+"/"+id;
        byte[] data = ZkUtils.getData(demarcationPointPath);
        long demarcationPoint = data != null ? Bytes.toLong(data) : 0;
        SpliceLogUtils.info(LOG, "Get DDL demarcation point %d from zookeeper", demarcationPoint);

        return demarcationPoint;
    }
}
