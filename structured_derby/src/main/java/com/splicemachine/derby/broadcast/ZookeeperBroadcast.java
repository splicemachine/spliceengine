package com.splicemachine.derby.broadcast;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.ZkUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ZookeeperBroadcast {
    private static final Logger LOG = Logger.getLogger(ZookeeperBroadcast.class);
    private static final long REFRESH_TIMEOUT = 30000; // in ms
    private static final long MAXIMUM_WAIT = 60000; // maximum wait for everybody to respond, after this we fail the  broadcast

    private Map<String, MessageHandler> handlers;
    private final Object lock = new Object();
    private String id;
    private MessagesWatcher messagesWatcher = new MessagesWatcher();
    private ServersWatcher serversWatcher = new ServersWatcher();
    private Map<String, Set<String>> receivedMessages = new HashMap<String, Set<String>>();

    public ZookeeperBroadcast (Map<String, MessageHandler> handlers) {
        this.handlers = new ConcurrentHashMap<String, MessageHandler>(handlers);
        for (String topic : handlers.keySet()) {
            receivedMessages.put(topic, new HashSet<String>());
        }
    }

    public void start() throws StandardException {
        createZKTree();

        try {
            String node = ZkUtils.create(SpliceConstants.zkSpliceBroadcastActiveServersPath + "/", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            id = node.substring(node.lastIndexOf('/') + 1);
        } catch (KeeperException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }
        for (String topic : handlers.keySet()) {
            refresh(topic);
        }
    }

    public void stop() throws StandardException {
        try {
            ZkUtils.delete(SpliceConstants.zkSpliceBroadcastActiveServersPath + "/" + id);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    private synchronized void refresh(String topic) throws StandardException {
        // Get all inflight messages
        String topicPath = SpliceConstants.zkSpliceBroadcastMessagesPath + "/" + topic;
        List<String> children;
        try {
            children = ZkUtils.getChildren(topicPath, messagesWatcher);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        Set<String> messagesForTopic = receivedMessages.get(topic);
        Set<String> newMessages = new HashSet<String>();

        MessageHandler handler = handlers.get(topic);
        if (handler == null) {
            LOG.warn("No handler for topic " + topic);
            return;
        }

        // remove broadcasted messages
        for (Iterator<String> iterator = messagesForTopic.iterator(); iterator.hasNext();) {
            String msgId = iterator.next();
            if (!children.contains(msgId)) {
                // notify acknowledgement
                handler.messageAcknowledged(msgId);

                iterator.remove();
            }
        }
        for (Iterator<String> iterator = children.iterator(); iterator.hasNext();) {
            String msgId = iterator.next();
            if (!messagesForTopic.contains(msgId)) {
                byte[] data;
                try {
                    data = ZkUtils.getData(topicPath + "/" + msgId);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                messagesForTopic.add(msgId);

                // handle message
                handler.handleMessage(msgId, data);
            }
            // notify broadcaster we processed the message
            try {
                ZkUtils.create(topicPath + "/" + msgId + "/" + id,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    private void createZKTree() throws StandardException {
        List<String> paths = new ArrayList<String>();
        paths.add(SpliceConstants.zkSpliceBroadcastPath);
        paths.add(SpliceConstants.zkSpliceBroadcastActiveServersPath);
        paths.add(SpliceConstants.zkSpliceBroadcastMessagesPath);
        for (String topic : handlers.keySet()) {
            paths.add(SpliceConstants.zkSpliceBroadcastMessagesPath + "/" + topic);
        }
        for (String path : paths) {
            try {
                ZkUtils.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                // ignore NODE EXISTS, otherwise throw exception
                if (!e.code().equals(Code.NODEEXISTS)) {
                    throw Exceptions.parseException(e);
                }
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    public void broadcastMessage(String topic, byte[] data) throws StandardException {
        String messageId;
        try {
            messageId = ZkUtils.create(SpliceConstants.zkSpliceBroadcastMessagesPath + "/" + topic + "/", data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }

        long startTimestamp = System.currentTimeMillis();
        synchronized (lock) {
            while (true) {
                Collection<String> servers;
                try {
                    servers = ZkUtils.getChildren(SpliceConstants.zkSpliceBroadcastActiveServersPath, serversWatcher);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                List<String> children;
                try {
                    children = ZkUtils.getChildren(messageId, serversWatcher);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                boolean missing = false;
                for (String server : servers) {
                    if (!children.contains(server)) {
                        missing = true;
                        break;
                    }
                }

                if (!missing) {
                    // everybody responded, leave loop
                    break;
                }

                if (System.currentTimeMillis() - startTimestamp > MAXIMUM_WAIT) {
                    LOG.error("Maximum wait for all servers exceeded. Waiting response from " + servers
                            + ". Received response from " + children);
                    try {
                        ZkUtils.recursiveDelete(messageId);
                    } catch (Exception e) {
                        LOG.error("Couldn't remove failed broadcast with id " + messageId, e);
                    }
                    throw StandardException.newException(org.apache.derby.shared.common.reference.SQLState.LOCK_TIMEOUT,  "Wait of "
                            + (System.currentTimeMillis() - startTimestamp) + " exceeded timeout of " + MAXIMUM_WAIT);
                }
                try {
                    lock.wait(REFRESH_TIMEOUT);
                } catch (InterruptedException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }
        try {
            ZkUtils.recursiveDelete(messageId);
        } catch (Exception e) {
            LOG.error("Couldn't remove message " + messageId, e);
        }
    }

    private class MessagesWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType().equals(EventType.NodeChildrenChanged)) {
                String path = event.getPath();
                String topic = path.substring(path.lastIndexOf('/') + 1);
                try {
                    refresh(topic);
                } catch (StandardException e) {
                    LOG.error("Couldn't process new message for topic " + topic, e);
                }
            }
        }
    }

    private class ServersWatcher implements Watcher {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType().equals(EventType.NodeChildrenChanged)) {
                synchronized (lock) {
                    lock.notify();
                }
            }
        }
    }
}

