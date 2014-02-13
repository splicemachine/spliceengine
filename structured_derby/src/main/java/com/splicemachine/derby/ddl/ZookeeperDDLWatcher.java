package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.ZkUtils;

public class ZookeeperDDLWatcher implements DDLWatcher, Watcher {
    private static final Logger LOG = Logger.getLogger(ZookeeperDDLWatcher.class);
    private static final long REFRESH_TIMEOUT = 30000; // in ms
    private static final long MAXIMUM_DDL_WAIT = 90000; // in ms

    private static Gson gson = new Gson();
    private Map<String, String> currentDDLChanges = new HashMap<String, String>();
    private Set<String> seenDDLChanges = new HashSet<String>();
    private Map<String, Long> changesTimeouts = new HashMap<String, Long>();
    private List<LanguageConnectionContext> contexts = new ArrayList<LanguageConnectionContext>();
    private String id;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, 
            new ThreadFactoryBuilder().setNameFormat("ZookeeperDDLWatcherRefresh").build());
    private Map<String, DDLChange> tentativeIndexes = new ConcurrentHashMap<String, DDLChange>();
    
    @Override
    public synchronized void registerLanguageConnectionContext(LanguageConnectionContext lcc) {
        contexts.add(lcc);
        if (!currentDDLChanges.isEmpty()) {
            lcc.startGlobalDDLChange();
        }
    }

    @Override
    public void start() throws StandardException {
        createZKTree();

        try {
            String node = ZkUtils.create(SpliceConstants.zkSpliceDDLActiveServersPath + "/", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            id = node.substring(node.lastIndexOf('/') + 1);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }

        // run refresh() synchronously the first time
        refresh();

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                } catch (StandardException e) {
                    LOG.error("Failed to execute refresh", e);
                }
            }
        }, REFRESH_TIMEOUT, REFRESH_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void createZKTree() throws StandardException {
        for (String path : new String[] { SpliceConstants.zkSpliceDDLPath,
                SpliceConstants.zkSpliceDDLOngoingTransactionsPath, SpliceConstants.zkSpliceDDLActiveServersPath }) {
            try {
                ZkUtils.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (e.code().equals(Code.NODEEXISTS)) {
                    // ignore
                } else {
                    throw Exceptions.parseException(e);
                }
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            try {
                refresh();
            } catch (StandardException e) {
                LOG.error("Couldn't process the ZooKeeper event " + event, e);
            }
        }
    }

    private synchronized void refresh() throws StandardException {
        // Get all ongoing DDL changes
        List<String> children;
        try {
            children = ZkUtils.getChildren(SpliceConstants.zkSpliceDDLOngoingTransactionsPath, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        boolean currentWasEmpty = currentDDLChanges.isEmpty();
        Set<String> newChanges = new HashSet<String>();

        // remove finished ddl changes
        for (Iterator<String> iterator = seenDDLChanges.iterator(); iterator.hasNext();) {
            String entry = iterator.next();
            if (!children.contains(entry)) {
                LOG.debug("Removing change with id " + entry);
                changesTimeouts.remove(entry);
                currentDDLChanges.remove(entry);
                tentativeIndexes.remove(entry);
                iterator.remove();
            }
        }
        for (Iterator<String> iterator = children.iterator(); iterator.hasNext();) {
            String changeId = iterator.next();
            if (!seenDDLChanges.contains(changeId)) {
                byte[] data;
                try {
                    data = ZkUtils.getData(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + changeId);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                String jsonChange = Bytes.toString(data);
                LOG.debug("New change with id " + changeId + " :" + jsonChange);
                DDLChange ddlChange = gson.fromJson(jsonChange, DDLChange.class);
                ddlChange.setIdentifier(changeId);
                newChanges.add(changeId);
                seenDDLChanges.add(changeId);
                if (ddlChange.isTentative()) {
                    processTentativeDDLChange(changeId, ddlChange);
                } else {
                    currentDDLChanges.put(changeId, ddlChange.getTransactionId());
                    changesTimeouts.put(changeId, System.currentTimeMillis());
                }
            }
        }

        if (currentWasEmpty != currentDDLChanges.isEmpty()) {
            if (currentDDLChanges.isEmpty()) {
                LOG.debug("Finishing global ddl changes ");
                // we can use caches again
                for (LanguageConnectionContext c : contexts) {
                    c.finishGlobalDDLChange();
                }
            } else {
                LOG.debug("Starting global ddl changes, invalidate and disable caches");
                // we have to invalidate and disable caches
                for (LanguageConnectionContext c : contexts) {
                    c.startGlobalDDLChange();
                }
            }
        }

        // notify ddl operation we processed the change
        for (String change : newChanges) {
            try {
                ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + change + "/" + id,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                switch(e.code()) {
                    case NODEEXISTS: //we may have already set the value, so ignore node exists issues
                    case NONODE: // someone already removed the notification, it's obsolete
                        // ignore
                        break;
                    default:
                        throw Exceptions.parseException(e);
                }
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }

        for (Entry<String, Long> entry : changesTimeouts.entrySet()) {
            if (System.currentTimeMillis() > entry.getValue() + MAXIMUM_DDL_WAIT) {
                killDDLTransaction(entry.getKey());
            }
        }
    }

    private void processTentativeDDLChange(String changeId, DDLChange ddlChange) throws StandardException {
        switch (ddlChange.getType()) {
            case CREATE_INDEX:
                tentativeIndexes.put(changeId, ddlChange);
                break;
            default:
                throw StandardException.newException(SQLState.UNSUPPORTED_TYPE);
        }
    }

    private void killDDLTransaction(String changeId) {
        try {
            String transactionId = currentDDLChanges.get(changeId);
            LOG.warn("We are killing transaction " + transactionId + " since it exceeds the maximum wait period for"
                    + " the DDL change " + changeId + " publication");
            HTransactorFactory.getTransactor().fail(new TransactionId(transactionId));
        } catch (Exception e) {
            LOG.warn("Couldn't kill transaction, already killed?", e);
        }
        final String changePath = SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + changeId;
        try {
            ZkUtils.recursiveDelete(changePath);
        } catch (Exception e) {
            LOG.error("Couldn't kill transaction for DDL change " + changeId, e);
        }
    }

    public Set<DDLChange> getTentativeIndexes() {
        return new HashSet<DDLChange>(tentativeIndexes.values());
    }
}
