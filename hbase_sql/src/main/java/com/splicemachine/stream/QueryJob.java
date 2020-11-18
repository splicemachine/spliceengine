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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.function.CloneFunction;
import com.splicemachine.derby.stream.function.IdentityFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.internal.SQLConf;

import java.util.UUID;
import java.util.concurrent.*;

import static com.splicemachine.derby.impl.sql.execute.TriggerRowHolderImpl.dropTable;
import static java.lang.String.format;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class QueryJob implements Callable<Void>{

    private static final Logger LOG = Logger.getLogger(QueryJob.class);

    private final OlapStatus status;
    private final RemoteQueryJob queryRequest;

    public QueryJob(RemoteQueryJob queryRequest,
                    OlapStatus jobStatus) {
        this.status = jobStatus;
        this.queryRequest = queryRequest;
    }

    public static void setSparkContextInLCC(SparkContext sparkContext,
                                            LanguageConnectionContext lcc,
                                            int applicationJarsHash) {
        if (lcc == null)
            return;

        lcc.setSparkContext(sparkContext);
        lcc.setApplicationJarsHashCode(applicationJarsHash);
    }

    public static LanguageConnectionContext getLCC() {
        return (LanguageConnectionContext) ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
    }



    @Override
    public Void call() throws Exception {
        if(!status.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        ActivationHolder ah = queryRequest.ah;
        SpliceOperation root = ah.getOperationsMap().get(queryRequest.rootResultSetNumber);
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        DataSet<ExecRow> dataset;
        OperationContext<SpliceOperation> context;
        String jobName = null;
        Activation activation = null;
        boolean resetSession = false;
        LanguageConnectionContext lcc = null;
        int initialApplicationJarsHash = 0;

        try {
            if (queryRequest.shufflePartitions != null) {
                SpliceSpark.getSession().conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), queryRequest.shufflePartitions);
                resetSession = true;
            }
            ah.reinitialize(null);
            activation = ah.getActivation();
            if (activation != null) {
                lcc = activation.getLanguageConnectionContext();
                JavaSparkContext jsc = SpliceSpark.getContext();
                if (jsc != null) {
                    initialApplicationJarsHash = SpliceSpark.getApplicationJarsHash();
                    setSparkContextInLCC(jsc.sc(), lcc, initialApplicationJarsHash);
                }
            }
            root.setActivation(activation);
            if (!(activation.isMaterialized()))
                activation.materialize();
            TxnView parent = root.getCurrentTransaction();
            long txnId = parent.getTxnId();
            String sql = queryRequest.sql;
            String session = queryRequest.session;
            String userId = queryRequest.userId;
            jobName = userId + " <" + session + "," + txnId + ">";

            LOG.info("Running query for user/session: " + userId + "," + session);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Query: " + sql);
            }

            dsp.setJobGroup(jobName, sql);
            addUserJarsToSparkContext(activation, SpliceSpark.getContext());
            dataset = root.getDataSet(dsp);
            if (lcc != null) {
                int applicationJarsHash;
                LanguageConnectionContext lccFromContext = getLCC();
                if (lccFromContext != null) {
                    applicationJarsHash = lccFromContext.getApplicationJarsHashCode();
                    if (applicationJarsHash != 0 &&
                        applicationJarsHash != initialApplicationJarsHash)
                        SpliceSpark.setApplicationJarsHash(applicationJarsHash);
                }
                else {
                    applicationJarsHash = lcc.getApplicationJarsHashCode();
                    if (applicationJarsHash != 0 &&
                        applicationJarsHash != initialApplicationJarsHash)
                        SpliceSpark.setApplicationJarsHash(applicationJarsHash);
                }
            }
            context = dsp.createOperationContext(root);
            SparkDataSet<ExecRow> sparkDataSet = (SparkDataSet<ExecRow>) dataset
                    .map(new CloneFunction<>(context))
                    .map(new IdentityFunction<>(context)); // force materialization into Derby's format
            String clientHost = queryRequest.host;
            int clientPort = queryRequest.port;
            UUID uuid = queryRequest.uuid;
            int numPartitions = sparkDataSet.rdd.rdd().getNumPartitions();

            JavaRDD rdd =  sparkDataSet.rdd;
            StreamableRDD streamableRDD = new StreamableRDD<>(rdd, context, uuid, clientHost, clientPort,
                    queryRequest.streamingBatches, queryRequest.streamingBatchSize,
                    queryRequest.parallelPartitions);
            streamableRDD.setJobStatus(status);
            streamableRDD.submit();

            status.markCompleted(new QueryResult(numPartitions));

            LOG.info("Completed query for session: " + session);
        } catch (CancellationException e) {
            if (jobName != null)
                SpliceSpark.getContext().sc().cancelJobGroup(jobName);
            throw e;
        } finally {
            long tempTriggerConglomerate = dsp.getTempTriggerConglomerate();
            if (tempTriggerConglomerate != 0 && activation != null)
                dropConglomerate(tempTriggerConglomerate, activation);
            if(resetSession)
                SpliceSpark.resetSession();
            ah.close();
        }

        return null;
    }

    // Tell Spark where to find user jars that were
    // added via CALL SQLJ.INSTALL_JAR.
    private void addUserJarsToSparkContext(Activation activation, JavaSparkContext jsc) {
        if (jsc == null)
            return;

        SparkContext sparkContext = jsc.sc();

        if (sparkContext == null)
            return;

        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();
        if (lcc == null)
            return;
        lcc.setSparkContext(sparkContext);
        lcc.addUserJarsToSparkContext();
    }


    private void dropConglomerate(long CID, Activation activation) {
        TransactionController tc = activation.getTransactionController();
        LOG.trace(format("Dropping temporary conglomerate splice:%d", CID));
        try {
            tc.dropConglomerate(CID);
        }
        catch (StandardException e) {
            LOG.warn(format("Unable to drop temporary trigger conglomerate %d.  Cleanup may have been called twice.", CID), e);
        }
        try {
            dropTable(CID);
        }
        catch (StandardException e) {
            LOG.warn(format("Unable to drop HBase table for temporary trigger conglomerate %d.  Cleanup may have been called twice.", CID), e);
        }
    }
}
