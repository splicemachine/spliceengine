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

package com.splicemachine.stream;

import com.google.common.net.HostAndPort;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.si.constants.SIConstants;
import io.netty.channel.ChannelHandler;
import org.apache.log4j.Logger;
import splice.com.google.common.util.concurrent.ListenableFuture;
import splice.com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.*;



/**
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class RemoteQueryClientImpl implements RemoteQueryClient {
    private static final Logger LOG = Logger.getLogger(RemoteQueryClientImpl.class);

    private static StreamListenerServer server;

    private final SpliceBaseOperation root;
    private final String hostname;
    private ListenableFuture<OlapResult> olapFuture;
    private StreamListener streamListener;
    private long offset = 0;
    private long limit = -1;

    public RemoteQueryClientImpl(SpliceBaseOperation root, String hostname) {
        this.root = root;
        this.hostname = hostname;
    }

    private StreamListenerServer getServer() throws StandardException {
        synchronized (RemoteQueryClientImpl.class) {
            if (server == null) {
                server = new StreamListenerServer(0);
                server.start();
            }
        }
        return server;
    }

    private int getPropertyOrDefault(Activation activation, SessionProperties.PROPERTYNAME valueProperty, int defaultValue) {
        int value;
        Integer batchesProperty = (Integer) activation.getLanguageConnectionContext().getSessionProperties()
                .getProperty(valueProperty);
        if (batchesProperty != null) {
            value = batchesProperty.intValue();
        } else {
            value = defaultValue;
        }
        return value;
    }

    @Override
    public void submit() throws StandardException {
        Activation activation = root.getActivation();
        ActivationHolder ah = new ActivationHolder(activation, root);

        try {
            updateLimitOffset();
            SConfiguration config = HConfiguration.getConfiguration();
            boolean hasLOBs = hasLOBs(root);
            int streamingBatches = getPropertyOrDefault(activation, SessionProperties.PROPERTYNAME.SPARK_RESULT_STREAMING_BATCHES,
                    hasLOBs ? config.getSparkSlowResultStreamingBatches() : config.getSparkResultStreamingBatches());
            int streamingBatchSize = getPropertyOrDefault(activation, SessionProperties.PROPERTYNAME.SPARK_RESULT_STREAMING_BATCH_SIZE,
                    hasLOBs ? config.getSparkSlowResultStreamingBatchSize() : config.getSparkResultStreamingBatchSize());
            streamListener = new StreamListener(limit, offset, streamingBatches, streamingBatchSize);
            StreamListenerServer server = getServer();
            server.register(streamListener);
            HostAndPort hostAndPort = server.getHostAndPort();
            String host = hostAndPort.getHostText();
            int port = hostAndPort.getPort();
            UUID uuid = streamListener.getUuid();

            String sql = activation.getPreparedStatement().getSource();
            sql = sql == null ? root.toString() : sql;
            LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
            String userId = lcc.getCurrentUserId(activation);
            int localPort = config.getNetworkBindPort();
            int sessionId = lcc.getInstanceNumber();
            Integer parallelPartitionsProperty = (Integer) lcc.getSessionProperties()
                    .getProperty(SessionProperties.PROPERTYNAME.OLAPPARALLELPARTITIONS);
            int parallelPartitions = parallelPartitionsProperty == null ? StreamableRDD.DEFAULT_PARALLEL_PARTITIONS : parallelPartitionsProperty;
            Integer shufflePartitionsProperty = (Integer) lcc.getSessionProperties()
                    .getProperty(SessionProperties.PROPERTYNAME.OLAPSHUFFLEPARTITIONS);
            String opUuid = root.getUuid() != null ? "," + root.getUuid().toString() : "";
            String session = hostname + ":" + localPort + "," + sessionId + opUuid;

            RemoteQueryJob jobRequest = new RemoteQueryJob(ah, root.getResultSetNumber(), uuid, host, port, session, userId, sql,
                    streamingBatches, streamingBatchSize, parallelPartitions, shufflePartitionsProperty);

            String requestedQueue = (String) lcc.getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.OLAPQUEUE);
            String queue = chooseQueue(activation, requestedQueue, config.getOlapServerIsolatedRoles());
            olapFuture = EngineDriver.driver().getOlapClient().submit(jobRequest, queue);
            olapFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        OlapResult olapResult = olapFuture.get();
                        streamListener.completed(olapResult);
                    } catch (ExecutionException e) {
                        LOG.warn("Execution failed", e);
                        Throwable cause = e.getCause();
                        if (cause instanceof IOException) { // including ConnectException
                            streamListener.failed(StandardException.newException(SQLState.OLAP_SERVER_CONNECTION, cause));
                        } else {
                            streamListener.failed(cause);
                        }
                    } catch (InterruptedException e) {
                        // this shouldn't happen, the olapFuture already completed
                        Thread.currentThread().interrupt();
                        LOG.error("Unexpected exception, shouldn't happen", e);
                        streamListener.failed(e);
                    }
                }
            }, MoreExecutors.sameThreadExecutor());
        } catch (IOException e) {
            throw StandardException.newException(SQLState.OLAP_SERVER_CONNECTION, e);
        }
    }

    private boolean hasLOBs(SpliceBaseOperation root) throws StandardException {
        if (root instanceof ScrollInsensitiveOperation) {
            for (ResultColumnDescriptor descriptor : root.getActivation().getResultDescription().getColumnInfo()) {
                if (descriptor.getType().getTypeId().isBlobTypeId() || descriptor.getType().getTypeId().isClobTypeId()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * If user is current data base owner:
     *      - if `requestedQueue` is not null -> return it
     *      - if `requestedQueue` is null -> return the default queue.
     * If user is normal user:
     *      - If requestedQueue is null, return the assigned queue for any of the active roles.
     *        If none match return the default queue
     *      - If requestedQueue is not null, make sure the requestedQueue is assigned to any of this users's roles and
     *        return it if there's a match. If it's not return the default queue with the side effect of refreshing
     *        the list of user roles.
     */
    private String chooseQueue(Activation activation, String requestedQueue,
                               Map<String, String> olapServerIsolatedRoles) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

        List<String> userGroups = lcc.getCurrentGroupUser(activation);
        String dbo = lcc.getDataDictionary().getAuthorizationDatabaseOwner();
        if(lcc.getCurrentUserId(activation).equals(dbo) || (userGroups != null && userGroups.contains(dbo))) {
            if(requestedQueue != null) {
                return requestedQueue;
            }
        } else {
            // remove any stale revoked roles (DB-9749)
            lcc.refreshCurrentRoles(activation);
            List<String> roles = lcc.getCurrentRoles(activation);
            if (requestedQueue != null) {
                // make sure the requested queue is available for the user roles
                for (String role: roles) {
                    if (requestedQueue.equals(olapServerIsolatedRoles.get(role))) {
                        return requestedQueue;
                    }
                }
            } else {
                for (String role : roles) {
                    if (olapServerIsolatedRoles.get(role) != null) {
                        return olapServerIsolatedRoles.get(role);
                    }
                }
            }
        }
        return SIConstants.OLAP_DEFAULT_QUEUE_NAME;
    }

    private void updateLimitOffset() throws StandardException {
        if (root instanceof ScrollInsensitiveOperation
                || root instanceof AnyOperation
                || root instanceof OnceOperation) {

            SpliceOperation source = root.getSubOperations().get(0);
            if (!(source instanceof RowCountOperation))
                return;

            RowCountOperation rco = (RowCountOperation) source;
            this.limit = rco.getFetchLimit();
            this.offset = rco.getTotalOffset();
            if (this.offset == -1) {
                offset = 0;
            }
            rco.setBypass(); // bypass this RowCountOperation
        }
    }

    @Override
    public Iterator<ExecRow> getIterator() {
        return streamListener.getIterator();
    }

    @Override
    public void interrupt() {
        streamListener.stopAllStreams();
        if (olapFuture != null)
            olapFuture.cancel(true);
    }

    @Override
    public void close() throws Exception {
        streamListener.stopAllStreams();
        if (olapFuture != null)
            olapFuture.cancel(false);
    }
}
