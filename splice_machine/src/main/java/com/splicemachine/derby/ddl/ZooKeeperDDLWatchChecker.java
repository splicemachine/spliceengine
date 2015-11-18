package com.splicemachine.derby.ddl;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.ZkUtils;
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
    private static final String CHANGES_PATH = SpliceConstants.zkSpliceDDLOngoingTransactionsPath;
    private static final Logger LOG=Logger.getLogger(ZooKeeperDDLWatchChecker.class);
    private String id;
    private Watcher changeIdWatcher;

    @Override
    public boolean initialize(final CommunicationListener changeIdListener) throws IOException{
        DDLZookeeperClient.createRequiredZooNodes();

        this.id = DDLZookeeperClient.registerThisServer();
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
    public List<String> getCurrentChangeIds() throws IOException{
        try{
            return ZkUtils.getRecoverableZooKeeper().getChildren(CHANGES_PATH,changeIdWatcher);
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
        byte[] data = ZkUtils.getData(SpliceConstants.zkSpliceDDLOngoingTransactionsPath+"/"+changeId);
        return DDLMessage.DDLChange.newBuilder()
                .mergeFrom(DDLMessage.DDLChange.parseFrom(data))
                .setChangeId(changeId).build();
    }

    @Override
    public void notifyProcessed(Collection<DDLMessage.DDLChange> processedChanges) throws IOException{
        /*
         * Notify the relevant controllers that their change has been processed
         */
        List<Op> ops = Lists.newArrayListWithExpectedSize(processedChanges.size());
        List<DDLMessage.DDLChange> changeList = Lists.newArrayList();
        for (DDLMessage.DDLChange change : processedChanges) {
            String path=SpliceConstants.zkSpliceDDLOngoingTransactionsPath+"/"+change.getChangeId()+"/"+id;
            Op op = Op.create(path, new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
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
       DDLZookeeperClient.deleteChangeNode(key);
    }

}
