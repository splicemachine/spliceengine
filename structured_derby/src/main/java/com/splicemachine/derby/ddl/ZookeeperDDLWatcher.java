package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;

public class ZookeeperDDLWatcher implements DDLWatcher, Watcher {
    private static final Logger LOG = Logger.getLogger(ZookeeperDDLWatcher.class);

    private Set<String> currentDDLChanges = new HashSet<String>();
    private List<LanguageConnectionContext> contexts = new ArrayList<LanguageConnectionContext>();
    private String id;
    
    @Override
    public void registerLanguageConnectionContext(LanguageConnectionContext lcc) {
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
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            id = node.substring(node.lastIndexOf('/') + 1);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }

        List<String> children;
        try {
            children = ZkUtils.getChildren(SpliceConstants.zkSpliceDDLOngoingTransactionsPath, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        for (String change : children) {
            currentDDLChanges.add(change);
            try {
                ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + change + "/" + id,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                throw Exceptions.parseException(e);
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }
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

    private void refresh() throws StandardException {
        // Get all ongoing DDL changes
        List<String> children;
        try {
            children = ZkUtils.getChildren(SpliceConstants.zkSpliceDDLOngoingTransactionsPath, this);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        // remove finished ddl changes
        boolean changed = currentDDLChanges.retainAll(children);
        // are there new changes?
        HashSet<String> newChanges = new HashSet<String>(children);
        newChanges.removeAll(currentDDLChanges);

        changed = currentDDLChanges.addAll(children) || changed; // add all changes that currently exist
        if (changed && !currentDDLChanges.isEmpty()) {
            // we have to invalidate and disable caches
            for (LanguageConnectionContext c : contexts) {
                c.startGlobalDDLChange();
            }
        } else if (changed && currentDDLChanges.isEmpty()) {
            // we can use caches again
            for (LanguageConnectionContext c : contexts) {
                c.finishGlobalDDLChange();
            }
        }

        // notify ddl operation we processed the change
        for (String change : newChanges) {
            try {
                ZkUtils.create(SpliceConstants.zkSpliceDDLOngoingTransactionsPath + "/" + change + "/" + id,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                throw Exceptions.parseException(e);
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

}
