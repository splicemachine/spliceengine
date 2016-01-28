package com.splicemachine.ddl;

import com.google.common.collect.Lists;
import com.splicemachine.derby.ddl.CommunicationListener;
import com.splicemachine.derby.ddl.DDLConfiguration;
import com.splicemachine.derby.ddl.DDLWatchChecker;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

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


        return true;
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
        byte[] data = ZkUtils.getData(zkClient.changePath+"/"+changeId);
        return DDLMessage.DDLChange.newBuilder()
                .mergeFrom(DDLMessage.DDLChange.parseFrom(data))
                .setChangeId(changeId).build();
    }

    @Override
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
            String path=zkClient.changePath+"/"+change.getChangeId()+"/"+(errorMessage==null?"":DDLConfiguration.ERROR_TAG)+id;
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
                    switch(code){
                        case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                        case NONODE: // someone already removed the notification, it's obsolete
                            // ignore
                            break;
                        default:
                            throw Exceptions.getIOException(KeeperException.create(code));
                    }
                }
            }
        } catch (InterruptedException e) {
            throw Exceptions.getIOException(e);
        } catch (KeeperException e) {
            switch(e.code()) {
                case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                case NONODE: // someone already removed the notification, it's obsolete
                    // ignore
                    break;
                default:
                    throw Exceptions.getIOException(e);
            }
        }
    }

    @Override
    public void killDDLTransaction(String key){
       zkClient.deleteChangeNode(key);
    }

}
