/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Scott Fines
 *         Date: 9/13/16
 */
class ServerList{
    private static final Logger LOGGER=Logger.getLogger(ServerList.class.getName());

    private final Set<String> blacklist=new CopyOnWriteArraySet<>();

    private final ServerLoop activeServers;

    ServerList(ConnectionSelectionStrategy css,ServerPool[] initialServers){
        //defensive copy necessary to protect the locked mutation idiom for bad usage
        this.activeServers=new ServerLoop(css,Arrays.copyOf(initialServers,initialServers.length));
    }

    Set<String> liveServers() throws SQLException{
        ServerPool[] sp = activeServers.array; //volatile read
        Set<String> servers = new HashSet<>(sp.length);
        for(ServerPool s:sp){
            if(s.isDead())
                serverDead(s);
            else
                servers.add(s.serverName);
        }
        return servers;
    }


    /**
     * Get an iterator of the <em>known active</em> servers <em>at the time of the request</em>. This is
     * a "snapshot in time" kind of thing; a server may become available immediately after this method returns, or
     * may become bad immediately after. Thus, callers should still respect error semantics (i.e. catching
     * network errors and reacting appropriately). The basic idiom should be
     * <p>
     * for(ServerPool sp:serverList.activeServers()){
     * if(sp.isDead()){
     * serverList.serverDead(sp);
     * }else{
     * //process
     * }
     * }
     * <p>
     * The returned iterator will begin at the "next" server to be tried (in a global sense). This "next"
     * is determined by the configured server selection strategy (Round Robin, Random, weighted random, etc.)
     *
     * @return an iterator of known active servers for this list.
     */
    Iterator<ServerPool> activeServers(){
        return activeServers.iterate();
    }

    /**
     * Used to indicate that a server that the list *thought* was alive
     * was actually dead.
     */
    void serverDead(ServerPool sp) throws SQLException{
        if(blacklist.add(sp.serverName)){
            activeServers.remove(sp);
            try{
                sp.close();
            }catch(SQLException e){
                if(!ClientErrors.isNetworkError(e))
                    throw e;
                else logError("serverDead",e);
            }
        }
    }

    void addServer(ServerPool newPool){
        if(activeServers.add(newPool)){
            blacklist.remove(newPool.serverName);
        }
    }

    /**
     * Merge a new set of servers into the list.
     *
     * @param newServers the new set of servers
     * @throws SQLException if something goes wrong during the setting.
     */
    void setServerList(ServerPool[] newServers) throws SQLException{
        activeServers.mergeArray(newServers);
    }

    /**
     * Validate which servers are alive and which are dead.
     * <p>
     * Note that this forcibly validates the connection to <em>every</em>
     * server in the list, and can therefore be quite expensive.
     * <p>
     * If a validation is currently in progress, this will block until that
     * progress is completed, then return without performing work.
     */
    void validateAllServers() throws SQLException{
        activeServers.heartbeatAll();
    }

    ServerPool[] clear(){
        blacklist.clear();
        return activeServers.clear();
    }

    Collection<String> blacklist(){
        return Collections.unmodifiableSet(blacklist);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void logError(String operation,Throwable t){
        logError(operation,t,Level.SEVERE);
    }

    private void logError(String operation,Throwable t,Level logLevel){
        String errorMessage="error during "+operation+":";
        if(t instanceof SQLException){
            SQLException se=(SQLException)t;
            errorMessage+="["+se.getSQLState()+"]";
        }
        if(t.getMessage()!=null)
            errorMessage+=t.getMessage();

        LOGGER.log(logLevel,errorMessage,t);
    }

    private class ServerLoop{
        private final ConnectionSelectionStrategy css;
        private final AtomicInteger position=new AtomicInteger(0);
        private final ReentrantLock mutationLock=new ReentrantLock();
        private volatile ServerPool[] array;

        private volatile AtomicReference<FutureTask<Void>> heartbeatTask=new AtomicReference<>(null);
        private final Callable<Void> heartbeat=new Callable<Void>(){
            @Override
            @SuppressWarnings("ForLoopReplaceableByForEach")
            public Void call() throws Exception{
                ServerPool[] cArray=array;
                //can't use foreach here because we are removing elements, causing concurrent modification issues
                for(int i=0;i<cArray.length;i++){
                    ServerPool sp=cArray[i];
                    try{
                        if(!sp.heartbeat()){
                            if(sp.isDead()){
                                serverDead(sp);
                            }
                        }
                    }catch(SQLException e){
                        if(!ClientErrors.isNetworkError(e)) throw e;
                        //otherwise we can ignore, because the ServerPool logic will mark it as failed for us
                    }
                }
                return null;
            }
        };

        ServerLoop(ConnectionSelectionStrategy css,ServerPool[] initialArray){
            this.css=css;
            Arrays.sort(initialArray);
            this.array=initialArray;
        }

        public ServerPool[] clear(){
            mutationLock.lock();
            try{
                ServerPool[] sp = array;
                array = null;
                return sp;
            }finally{
                mutationLock.unlock();
            }
        }

        public boolean add(ServerPool newPool){
            mutationLock.lock();
            try{
                ServerPool[] sp = array; //volatile read
                //the array is sorted, so use binary search to find its insertion point
                int pos=Arrays.binarySearch(sp,newPool);
                if(pos>=0 && sp[pos].serverName.equals(newPool.serverName)) return false; //we already have it

                //we have to add it in
                ServerPool[] newArray = new ServerPool[sp.length+1];
                if(pos<0){
                    newArray[0] = newPool; //it's inserted at the beginning
                    System.arraycopy(sp,0,newArray,1,sp.length);
                }else if(pos<sp.length){
                    System.arraycopy(sp,0,newArray,0,pos);
                    newArray[pos] = newPool;
                    System.arraycopy(sp,pos+1,newArray,pos+1,sp.length-pos);
                }else{
                    System.arraycopy(sp,0,newArray,0,sp.length);
                    newArray[sp.length] = newPool;
                }
                array = newArray;
                return true;
            }finally{
                mutationLock.unlock();
            }
        }

        void heartbeatAll() throws SQLException{
            FutureTask<Void> running;
            do{
                running=heartbeatTask.get();
                if(running!=null){
                    try{
                        running.get();
                    }catch(InterruptedException e){
                        Thread.currentThread().interrupt();
                    }catch(ExecutionException e){
                        logError("heartbeatAll",e.getCause());
                    }
                    return;
                }else{
                    FutureTask<Void> potentialTask=new FutureTask<>(heartbeat);
                    if(heartbeatTask.compareAndSet(null,potentialTask)){
                        running=potentialTask;
                        break;
                    }
                }
            }while(true);
            running.run();
            heartbeatTask.set(null);
        }

        Iterator<ServerPool> iterate(){
            ServerPool[] currArray=array; //volatile read
            return new LoopIterator(currArray,css.nextServer(position.incrementAndGet(),currArray.length));
        }

        void mergeArray(ServerPool[] servers) throws SQLException{
            mutationLock.lock();
            try{
                ServerPool[] oldServers=array;
                Arrays.sort(servers);
                int newPos=0;
                int oldPos=0;
                List<ServerPool> destServers=new ArrayList<>(oldServers.length); //most likely, the result will be the same size as before
                int minLen=Math.min(servers.length,oldServers.length);
                SQLException closeExceptions=null;
                while(newPos<minLen && oldPos<minLen){
                    ServerPool newSp=servers[newPos];
                    ServerPool oldSp=servers[oldPos];
                    int c=newSp.compareTo(oldSp);
                    try{
                        ServerPool sp = null;
                        if(c<0){
                            destServers.add(newSp);
                            newPos++;
                            sp = newSp;
                        }else if(c==0){
                            /*
                             * Since both ServerPool objects are the same, we keep the old one (which likely
                             * has some connections), and remove the new one
                             */
                            destServers.add(oldSp);
                            oldPos++;
                            newPos++;
                            newSp.close();
                            sp = oldSp;
                        }else{
                            /*
                             * We skipped an old server, which means it's dead. Therefore, we remove it,
                             * and advance the pointer
                             */
                            oldSp.close();
                            oldPos++;
                        }
                        if(sp!=null){
                            blacklist.remove(sp.serverName);
                        }
                    }catch(SQLException se){
                        if(!ClientErrors.isNetworkError(se)){
                            if(closeExceptions==null) closeExceptions=se;
                            else se.setNextException(se);
                        }
                    }
                }
                if(newPos<servers.length){
                    //noinspection ManualArrayToCollectionCopy
                    for(int i=newPos;i<servers.length;i++){
                        destServers.add(servers[i]);
                    }
                }
                if(oldPos<oldServers.length){
                    //all these servers are dead, close them
                    for(int i=oldPos;i<oldServers.length;i++){
                        try{
                            oldServers[i].close();
                        }catch(SQLException se){
                            if(!ClientErrors.isNetworkError(se)){
                                if(closeExceptions==null) closeExceptions=se;
                                else se.setNextException(se);
                            }
                        }
                    }
                }

                ServerPool[] destination = new ServerPool[destServers.size()];
                this.array = destServers.toArray(destination);
                if(closeExceptions!=null)
                    logError("mergeArray",closeExceptions);
            }finally{
                mutationLock.unlock();
            }
        }

        boolean remove(ServerPool server){
            assert server!=null;
            return remove(server.serverName);
        }

        boolean remove(String serverName){
            mutationLock.lock();
            ServerPool[] cArray=array; //avoid extraneous volatile reads
            try{
                int p=-1;
                for(int i=0;i<cArray.length;i++){
                    if(cArray[i].serverName.equals(serverName)){
                        p=i;
                        break;
                    }
                }
                if(p<0) return false;
                ServerPool[] copy=new ServerPool[cArray.length-1];
                System.arraycopy(cArray,0,copy,0,p);
                System.arraycopy(cArray,p+1,copy,p,copy.length-p);
                this.array=copy;
                return true;
            }finally{
                mutationLock.unlock();
            }
        }

        private class LoopIterator implements Iterator<ServerPool>{
            private final ServerPool[] currArray;

            private int pos;
            private int remaining;

            private ServerPool next;

            LoopIterator(ServerPool[] currArray,int initialPos){
                this.currArray=currArray;
                this.pos=initialPos;
                this.remaining=currArray.length;
            }

            @Override
            public boolean hasNext(){
                if(next!=null) return true;

                while(remaining>0){
                    ServerPool sp=currArray[pos%currArray.length];
                    remaining--;
                    pos++;
                    if(!sp.isDead()){
                        next=sp;
                        break;
                    }

                    try{
                        serverDead(sp);
                    }catch(SQLException e){
                        if(!ClientErrors.isNetworkError(e)) throw new RuntimeException(e);
                        logError("iterator.next",e);
                    }
                }
                return next!=null;
            }

            @Override
            public ServerPool next(){
                if(!hasNext()) throw new NoSuchElementException();
                ServerPool sp=next;
                next=null;

                return sp;
            }

            @Override
            public void remove(){
                throw new UnsupportedOperationException();
            }
        }
    }

}
