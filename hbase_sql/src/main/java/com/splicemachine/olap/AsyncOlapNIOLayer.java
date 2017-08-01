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

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.pipeline.Exceptions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.log4j.Logger;
import org.spark_project.guava.util.concurrent.ExecutionList;
import org.spark_project.guava.util.concurrent.ListenableFuture;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public class AsyncOlapNIOLayer implements JobExecutor{
    private static final Logger LOG=Logger.getLogger(AsyncOlapNIOLayer.class);

    private final int maxRetries;
    private final ChannelPool channelPool;
    private final ScheduledExecutorService executorService;
    private final ProtobufDecoder decoder=new ProtobufDecoder(OlapMessage.Response.getDefaultInstance(),buildExtensionRegistry());

    private ExtensionRegistry buildExtensionRegistry(){
        ExtensionRegistry er=ExtensionRegistry.newInstance();
        er.add(OlapMessage.FailedResponse.response);
        er.add(OlapMessage.CancelledResponse.response);
        er.add(OlapMessage.ProgressResponse.response);
        er.add(OlapMessage.Result.response);
        return er;
    }


    public AsyncOlapNIOLayer(String host, int port, int retries){
        maxRetries = retries;
        InetSocketAddress socketAddr=new InetSocketAddress(host,port);
        Bootstrap bootstrap=new Bootstrap();
        NioEventLoopGroup group=new NioEventLoopGroup(5,
                new ThreadFactoryBuilder().setNameFormat("olapClientWorker-%d").setDaemon(true)
                        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                            @Override
                            public void uncaughtException(Thread t, Throwable e) {
                                LOG.error("["+t.getName()+"] Unexpected error in AsyncOlapNIO pool: ",e);
                            }
                        }).build());
        bootstrap.channel(NioSocketChannel.class)
                .group(group)
                .option(ChannelOption.SO_KEEPALIVE,true)
                .remoteAddress(socketAddr);

        //TODO -sf- this may be excessive network usage --consider a bounded pool to prevent over-connection?
        this.channelPool=new SimpleChannelPool(bootstrap,new AbstractChannelPoolHandler(){
            @Override
            public void channelCreated(Channel channel) throws Exception{
                ChannelPipeline p=channel.pipeline();
                p.addLast("frameEncoder",new LengthFieldPrepender(4));
                p.addLast("protobufEncoder",new ProtobufEncoder());
                p.addLast("frameDecoder",new LengthFieldBasedFrameDecoder(1<<30,0,4,0,4));
                p.addLast("protobufDecoder",decoder);
            }
        });
        executorService = group;
    }

    @Override
    public ListenableFuture<OlapResult> submit(DistributedJob job) throws IOException{
        assert job.isSubmitted();
        if (LOG.isTraceEnabled())
            LOG.trace("Submitting job request "+ job.getUniqueName());
        OlapFuture future=new OlapFuture(job);
        future.doSubmit();
        return future;
    }


    @Override
    public void shutdown(){
        channelPool.close(); //disconnect everything
    }


    /* ****************************************************************************************************************/
    /*Private Helper methods and classes*/

    private OlapResult parseFromResponse(OlapMessage.Response response) throws IOException{
        switch(response.getType()){
            case NOT_SUBMITTED:
                return new NotSubmittedResult();
            case FAILED:
                OlapMessage.FailedResponse fr=response.getExtension(OlapMessage.FailedResponse.response);
                throw Exceptions.rawIOException((Throwable)OlapSerializationUtils.decode(fr.getErrorBytes()));
            case IN_PROGRESS:
                OlapMessage.ProgressResponse pr=response.getExtension(OlapMessage.ProgressResponse.response);
                return new SubmittedResult(pr.getTickTimeMillis());
            case CANCELLED:
                return new CancelledResult();
            case COMPLETED:
                OlapMessage.Result r=response.getExtension(OlapMessage.Result.response);
                return OlapSerializationUtils.decode(r.getResultBytes());
            default:
                throw new IllegalStateException("Programmer error: unexpected response type");
        }
    }

    private class OlapFuture implements ListenableFuture<OlapResult>, Runnable {
        private final DistributedJob job;
        private final Lock checkLock=new ReentrantLock();
        private final Condition signal=checkLock.newCondition();
        private final ChannelHandler resultHandler = new ResultHandler(this);
        private final ChannelHandler submitHandler = new SubmitHandler(this);
        private final ExecutionList executionList = new ExecutionList();
        private long lastStatus = System.currentTimeMillis(); // keeps track of last status received for logging

        private final GenericFutureListener<Future<Void>> failListener=new GenericFutureListener<Future<Void>>(){
            @Override
            public void operationComplete(Future<Void> future) throws Exception{
                if(!future.isSuccess()){
                    fail(future.cause());
                    signal();
                }
            }
        };

        private volatile OlapResult finalResult;
        private volatile boolean cancelled=false;
        private volatile boolean failed=false;
        private volatile boolean submitted=false;
        private volatile int notFound;
        private volatile Throwable cause=null;
        private volatile long tickTimeNanos=TimeUnit.MILLISECONDS.toNanos(1000L);
        private ScheduledFuture<?> keepAlive;
        private final ByteString data;

        OlapFuture(DistributedJob job) throws IOException {
            this.job=job;
            // We serialize it here because this can sometimes block (subquery materialization), and we don't want to block Netty's IO threads
            this.data=OlapSerializationUtils.encode(job);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning){
            if(isDone()) return false;
            else if(cancelled) return true;
            doCancel();

            /*
             *This is a bit weird, since we'll return true before we know whether or not it can be cancelled,
             *but the Future interface is awkward enough that that's the way it goes
             */
            return true;
        }

        @Override
        public boolean isCancelled(){
            return cancelled;
        }

        @Override
        public boolean isDone(){
            return cancelled || failed || finalResult!=null;
        }

        @Override
        public OlapResult get() throws InterruptedException, ExecutionException {
            try{
                return get(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }catch(TimeoutException e){
                //this will never happen, because we wait for forever. But just in case, wrap it in a runtime and
                //throw it anyway
                throw new RuntimeException(e);
            }
        }

        @Override
        public OlapResult get(long timeout,@Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException{
            long nanosRemaining=unit.toNanos(timeout);
            while(nanosRemaining>0){
                if(finalResult!=null)
                    return finalResult;
                else if(failed)
                    throw new ExecutionException(cause);
                else if(cancelled)
                    throw new CancellationException("Job " + job.getUniqueName() +" was cancelled.");
                if(Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                checkLock.lock();
                try{
                    if (!isDone()) {
                        long remaining = signal.awaitNanos(nanosRemaining);
                        nanosRemaining -= remaining;
                    }
                }finally{
                    checkLock.unlock();
                }
            }

            if(finalResult!=null)
                return finalResult;
            else if(failed)
                throw new ExecutionException(cause);

            throw new TimeoutException();
        }

        private void doCancel(){
            if (LOG.isTraceEnabled())
                LOG.trace("Cancelled job "+ job.getUniqueName());
            Future<Channel> channelFuture=channelPool.acquire();
            channelFuture.addListener(new CancelCommand(job.getUniqueName()));
            cancelled=true;
            signal();
        }

        void fail(Throwable cause){
            if (LOG.isTraceEnabled())
                LOG.trace("Failed job "+ job.getUniqueName() + " due to " + cause);
            this.cause=cause;
            this.failed=true;
            this.keepAlive.cancel(false);
            this.executionList.execute();
        }

        void success(OlapResult result) {
            if (LOG.isTraceEnabled())
                LOG.trace("Successful job "+ job.getUniqueName());
            this.finalResult = result;
            this.keepAlive.cancel(false);
            this.executionList.execute();
        }

        void doSubmit() throws IOException{
            Future<Channel> channelFuture=channelPool.acquire();
            if (LOG.isTraceEnabled())
                LOG.trace("Acquired channel");
            try{
                channelFuture.addListener(new SubmitCommand(this)).sync();
            }catch(InterruptedException ie){
                /*
                 * We were interrupted, which pretty much means that we are shutting down.
                 * Still, the API doesn't allow us to throw an Interrupt here, but we don't want to
                 * completely ignore it either. So we mark the current thread interrupted,
                 * then return (allowing early breakout without the exceptional error case).
                 */
                Thread.currentThread().interrupt();
                if (LOG.isTraceEnabled())
                    LOG.trace("Interrupted exception", ie);
            }

            this.keepAlive = executorService.scheduleWithFixedDelay(this, tickTimeNanos, tickTimeNanos, TimeUnit.NANOSECONDS);
        }

        void signal(){
            checkLock.lock();
            try{
                signal.signalAll();
            }finally{
                checkLock.unlock();
            }
        }

        @Override
        public void run() {
            if (submitted && !isDone()) {
                // don't request status until submitted
                Future<Channel> cFut = channelPool.acquire();
                cFut.addListener(new StatusListener(this));
            }
        }

        @Override
        public void addListener(Runnable runnable, Executor executor) {
            executionList.add(runnable, executor);
        }
    }

    private class SubmitCommand implements GenericFutureListener<Future<Channel>>{
        private OlapFuture olapFuture;

        SubmitCommand(OlapFuture olapFuture){
            this.olapFuture=olapFuture;
        }

        @Override
        public void operationComplete(Future<Channel> channelFuture) throws Exception{
            if (LOG.isTraceEnabled())
                LOG.trace("Submit command ");
            if(!channelFuture.isSuccess()){
                olapFuture.fail(channelFuture.cause());
                return;
            }
            final Channel c=channelFuture.getNow();
            ChannelPipeline writePipeline=c.pipeline();
            writePipeline.addLast("handler",olapFuture.submitHandler);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Submitted job " + olapFuture.job.getUniqueName());
            }

            OlapMessage.Submit submit=OlapMessage.Submit.newBuilder().setCommandBytes(olapFuture.data).build();
            OlapMessage.Command cmd=OlapMessage.Command.newBuilder()
                    .setUniqueName(olapFuture.job.getUniqueName())
                    .setExtension(OlapMessage.Submit.command,submit)
                    .setType(OlapMessage.Command.Type.SUBMIT)
                    .build();
            ChannelFuture writeFuture=c.writeAndFlush(cmd);
            writeFuture.addListener(olapFuture.failListener);

        }
    }

    private class StatusListener implements GenericFutureListener<Future<Channel>>{
        private final OlapFuture olapFuture;

        StatusListener(OlapFuture olapFuture){
            this.olapFuture=olapFuture;
        }

        @Override
        public void operationComplete(Future<Channel> channelFuture) throws Exception{
            if(!channelFuture.isSuccess()){
                olapFuture.fail(channelFuture.cause());
                return;
            }

            final Channel c=channelFuture.getNow();
            ChannelPipeline writePipeline=c.pipeline();
            writePipeline.addLast("handler",olapFuture.resultHandler);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Status check job " + olapFuture.job.getUniqueName());
            }

            OlapMessage.Status status=OlapMessage.Status.newBuilder().build();
            OlapMessage.Command cmd=OlapMessage.Command.newBuilder()
                    .setUniqueName(olapFuture.job.getUniqueName())
                    .setType(OlapMessage.Command.Type.STATUS)
                    .setExtension(OlapMessage.Status.command,status).build();
            ChannelFuture writeFuture=c.writeAndFlush(cmd);
            writeFuture.addListener(new GenericFutureListener<Future<Void>>(){
                @Override
                public void operationComplete(Future<Void> future) throws Exception{
                    //TODO -sf- possible failover/retry mechanism in place here?
                    if(!future.isSuccess()){
                        olapFuture.fail(future.cause());
                        olapFuture.signal();
                    }
                }
            });
        }
    }

    private class CancelCommand implements GenericFutureListener<Future<Channel>>{
        private String uniqueName;

        CancelCommand(String uniqueName){
            this.uniqueName=uniqueName;
        }

        @Override
        public void operationComplete(Future<Channel> channelFuture) throws Exception{
            if(!channelFuture.isSuccess()){
                 /*
                  * Unfortunately, no one is listening to this, so there's really no
                  * way to communicate this back to the client (the client has moved on).
                  * So just note the error and move on.
                  */
                LOG.error("Unable to cancel job "+uniqueName+": Unable to obtain channel",channelFuture.cause());
                return;
            }

            final Channel c=channelFuture.getNow();

            OlapMessage.Cancel cancel=OlapMessage.Cancel.newBuilder().build();
            OlapMessage.Command cmd=OlapMessage.Command.newBuilder()
                    .setUniqueName(uniqueName)
                    .setType(OlapMessage.Command.Type.CANCEL)
                    .setExtension(OlapMessage.Cancel.command,cancel).build();
            ChannelFuture writeFuture=c.writeAndFlush(cmd);
            writeFuture.addListener(new GenericFutureListener<Future<Void>>(){
                @Override
                public void operationComplete(Future<Void> future) throws Exception{
                    if(future.isSuccess()){
                        if(LOG.isTraceEnabled()){
                            LOG.trace("job "+uniqueName+" cancelled successfully");
                        }
                    }else{
                        LOG.error("Unable to cancel job "+uniqueName+": Unable to write cancel command",future.cause());
                    }
                    //once the write is complete, release the channel from the pool
                    channelPool.release(c);
                }
            });
        }
    }

    @ChannelHandler.Sharable
    private final class ResultHandler extends SimpleChannelInboundHandler<OlapMessage.Response> {
        private final OlapFuture future;

        ResultHandler(OlapFuture future){
            this.future=future;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx,OlapMessage.Response olapResult) throws Exception{
            OlapResult or=parseFromResponse(olapResult);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received " + or);
            }
            //TODO -sf- deal with a OlapServer failover here (i.e. a move to NOT_SUBMITTED from any other state
            if(or instanceof SubmittedResult) {
                future.tickTimeNanos = TimeUnit.MILLISECONDS.toNanos(((SubmittedResult) or).getTickTime());
                future.lastStatus = System.currentTimeMillis();
            } else if(future.submitted && !future.isDone() && or instanceof NotSubmittedResult) {
                // Server says the job is no longer submitted, give it a couple of tries in case messages are out of order
                long millisSinceLastStatus = System.currentTimeMillis() - future.lastStatus;
                LOG.warn("Status not available for job " + future.job.getUniqueName() +
                        ", millis since last status " + millisSinceLastStatus);
                if (future.notFound++ > maxRetries) {
                    // The job is no longer submitted, assume aborted
                    LOG.error("Failing job " + future.job.getUniqueName() + " after " + maxRetries +
                            " status not available responses");
                    future.fail(new IOException("Status not available, assuming aborted due to client timeout"));
                }
            }else if(or.isSuccess()){
                future.success(or);
            }else{
                // It should have a throwable
                Throwable t=or.getThrowable();
                if(t!=null){
                    future.fail(t);
                } else {
                    LOG.error("Message doesn't match any type of expected results: " + or);
                }
            }
            ctx.pipeline().remove(this); //we don't want this in the pipeline anymore
            Channel channel=ctx.channel();
            channelPool.release(channel); //release the underlying channel back to the pool cause we're done
            future.signal();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause) throws Exception{
            future.fail(cause);
            ctx.pipeline().remove(this); //we don't want this in the pipeline anymore
            channelPool.release(ctx.channel());
            future.signal();
        }
    }

    @ChannelHandler.Sharable
    private final class SubmitHandler extends SimpleChannelInboundHandler<OlapMessage.Response>{
        private final OlapFuture future;

        SubmitHandler(OlapFuture future){
            this.future=future;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx,OlapMessage.Response olapResult) throws Exception{
            OlapResult or=parseFromResponse(olapResult);
            if(or instanceof SubmittedResult) {
                future.tickTimeNanos = TimeUnit.MILLISECONDS.toNanos(((SubmittedResult) or).getTickTime());
                future.submitted = true;
            }else{
                Throwable t=or.getThrowable();
                LOG.error("Job wasn't submitted, result: " + or);
                if(t!=null){
                    future.fail(t);
                }else{
                    future.fail(new IOException("Job wasn't submitted, result: "+or));
                }
            }
            ctx.pipeline().remove(this); //we don't want this in the pipeline anymore
            Channel channel=ctx.channel();
            channelPool.release(channel); //release the underlying channel back to the pool cause we're done
            future.signal();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause) throws Exception{
            future.fail(cause);
            ctx.pipeline().remove(this); //we don't want this in the pipeline anymore
            channelPool.release(ctx.channel());
            future.signal();
        }
    }
}
