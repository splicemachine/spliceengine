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

package com.splicemachine.olap;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.olap.OlapMessage;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
class OlapRequestHandler extends AbstractOlapHandler{
    private static final Logger LOG =Logger.getLogger(OlapRequestHandler.class);

    private final ExecutorService executionPool;
    private final Clock clock;
    private final long clientCheckTimeMs;

    OlapRequestHandler(SConfiguration config,
                       OlapJobRegistry jobRegistry,
                       Clock clock,
                       long clientCheckTimeMs){
        super(jobRegistry);
        this.executionPool=configureThreadPool(config);
        this.clock=clock;
        this.clientCheckTimeMs=clientCheckTimeMs;
    }


    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception{
        final OlapMessage.Command jobRequest=((OlapMessage.Command)e.getMessage());
        assert jobRequest!=null;
        if(jobRequest.getType()!=OlapMessage.Command.Type.SUBMIT){
            ctx.sendUpstream(e);
            return;
        }
        OlapMessage.Submit extension=jobRequest.getExtension(OlapMessage.Submit.command);
        DistributedJob jr = OlapSerializationUtils.decode(extension.getCommandBytes());
        if(LOG.isTraceEnabled())
            LOG.trace("Submitting job request "+ jobRequest.getUniqueName());

        final OlapJobStatus jobStatus=jobRegistry.register(jobRequest.getUniqueName());

        OlapJobStatus.State state=jobStatus.currentState();
        switch(state){
            case SUBMITTED:
            case RUNNING:
            case CANCELED:
            case FAILED:
            case COMPLETE:
                if(LOG.isTraceEnabled())
                    LOG.trace("Job "+jobRequest.getUniqueName()+" already in progress, with state "+ state+", returning");
                writeResponse(e,jr.getUniqueName(),jobStatus);
                super.messageReceived(ctx,e);
                return;
            case NOT_SUBMITTED:
                if(LOG.isTraceEnabled())
                    LOG.trace("Attempting to submit job "+ jobRequest.getUniqueName());
                if(!jobStatus.markSubmitted()){
                    if(LOG.isTraceEnabled())
                        LOG.trace("Job submission for job "+jobRequest.getUniqueName()+" did not succeed, returning response");
                    writeResponse(e,jr.getUniqueName(),jobStatus);
                    super.messageReceived(ctx,e);
                    return;
                }
                break;
            default:
                throw new IllegalStateException("Unexpected job state: "+state);
        }
        final Callable<Void> job=jr.toCallable(jobStatus,clock,clientCheckTimeMs);

        executionPool.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    return job.call();
                } catch (Throwable t) {
                    LOG.error("Uncaught exception", t);
                    if (jobStatus.isRunning()) {
                        jobStatus.markCompleted(new FailedOlapResult(t));
                    }
                }
                return null;
            }
        });
        if(LOG.isTraceEnabled())
            LOG.trace("Job "+ jobRequest.getUniqueName()+" successfully submitted");
        writeResponse(e,jr.getUniqueName(),jobStatus);
    }



    /* ****************************************************************************************************************/
    /*private helper methods*/


    private ExecutorService configureThreadPool(SConfiguration config){
        //TODO -sf- bound this by the number of possible Spark tasks which can run in YARN
        ThreadFactory tf =new ThreadFactoryBuilder().setDaemon(true).setNameFormat("olap-worker-%d").build();
        return Executors.newCachedThreadPool(tf);
    }
}
