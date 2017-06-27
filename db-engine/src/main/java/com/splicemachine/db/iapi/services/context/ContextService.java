/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.services.context;

import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import org.apache.log4j.Logger;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

/**
 * A set of static methods to supply easier access to contexts.
 */
public final class ContextService{

    private static final Logger LOG = Logger.getLogger(ContextService.class);

    private static volatile ContextService factory;

    private HeaderPrintWriter errorStream;

    /**
     * Maintains a list of all the contexts that this thread has created
     * and/or used. The object stored in the thread local varys according
     * how this thread has been used and will be one of:
     * <p/>
     * <UL>
     * <LI> null - the thread no affiliation with a context manager.
     * <p/>
     * <LI> ContextManager - the current thread has used or is using
     * this context manager. If ContextManager.activeThread equals
     * the current thread then the thread is currently active with
     * the ContextManager. In this case ContextManager.activeCount
     * will be greater than zero and represent the level of nested
     * setCurrentContextmanager calls.
     * If ContextManager.activeThread is null then no other thread
     * is using the Contextmanager, if ContextManager.activeThread
     * is not-null and not equal to the current thread then some
     * other thread is using the context. It is assumed that
     * only a single thread can be using a ContextManager at any time
     * and this is enforced by synchronization outside the ContextManager.
     * E.g for JDBC connections, synchronization at the JDBC level.
     * <p/>
     * <LI> java.util.Stack containing ContextManagers - the current
     * thread is actively using multiple different ContextManagers,
     * with nesting. All ContextManagers in the stack will have
     * activeThread set to the current thread, and their activeCount
     * set to -1. This is beacause nesting is soley represented by
     * the stack, with the current context manager on top of the stack.
     * This supports multiple levels of nesting across two stacks, e.g.
     * C1->C2->C2->C1->C2.
     * </UL>
     * <p/>
     * This thread local is used to find the current context manager. Basically it provides
     * fast access to a list of candidate contexts. If one of the contexts has its activeThread
     * equal to the current thread then it is the current context manager.
     * <p/>
     * If the thread has pushed multiple contexts (e.g. open a new non-nested Derby connection
     * from a server side method) then threadContextList will contain a Stack. The value for each cm
     * will be a push order, with higher numbers being more recently pushed.
     * <p/>
     * To support the case where a single context manager is pushed twice (nested connection),
     * the context manager keeps track of the number of times it has been pushed (set). Note that
     * our synchronization requires that a single context can only be accessed by a single thread at a time.
     * In the JDBC layer this is enforced by the synchronization on the connection object.
     * <p/>
     * <p/>
     * There are two cases we are trying to optimise.
     * <UL>
     * <LI> Typical JDBC client program where there a Connection is always executed using a single thread.
     * In this case this variable will contain the Connection's context manager
     * <LI> Typical application server pooled connection where a single thread may use a connection from a pool
     * for the lifetime of the request. In this case this variable will contain a  WeakReference.
     * </UL>
     * <BR>
     * Single thread for Connection exection.
     * <pre>
     * threadContextList.get() == cm
     * // while in JDBC engine code
     * cm.activeThread == Thread.currentThread();
     * cm.activeCount = 1;
     * </pre>
     * <p/>
     * <BR>
     * J2EE single thread for lifetime of execution.
     * <pre>
     * // thread executing request
     * threadContextList.get() == cm
     * // while in JDBC engine code
     * cm.activeThread == Thread.currentThread();
     * cm.activeCount = 1;
     *
     * // other threads that have recently executed
     * // the same connection can have
     * threadContextList.get() == cm
     * cm.activeThread != Thread.currentThread();
     * </pre>
     * <p/>
     * <BR>
     * Nested routine calls within single connection
     * <pre>
     * threadContextList.get() == cm
     * // Within server-side JDBC code in a
     * // function called from another function/procedure
     * // called from an applications's statement
     * // (three levels of nesting)
     * cm.activeThread == Thread.currentThread();
     * cm.activeCount = 3;
     * </pre>
     * <p/>
     * <BR>
     * Nested routine calls with the inner routine
     * using a different connection to access a Derby database.
     * Note nesting of orignal Contextmanager cm is changed
     * from an activeCount of 2 to nesting within the stack
     * once multiple ContextManagers are involved.
     * <pre>
     * threadContextList.get() == stack {cm2,cm,cm}
     * cm.activeThread == Thread.currentThread();
     * cm.activeCount = -1; // nesting in stack
     * cm2.activeThread == Thread.currentThread();
     * cm2.activeCount = -1; // nesting in stack
     * </pre>
     * <p/>
     * <BR>
     * Nested multiple ContextManagers, the code supports
     * this, though it may not be possible currently
     * to have a stack like this from SQL/JDBC.
     * <pre>
     * threadContextList.get() == stack {cm3,cm2,cm,cm2,cm,cm}
     * cm.activeThread == Thread.currentThread();
     * cm.activeCount = -1; // nesting in stack
     * cm2.activeThread == Thread.currentThread();
     * cm2.activeCount = -1; // nesting in stack
     * cm3.activeThread == Thread.currentThread();
     * cm3.activeCount = -1; // nesting in stack
     * </pre>
     */

    private static class ContextManagerLink {
        ContextManager cm;
        ContextManagerLink next;

        ContextManagerLink(ContextManager cm, ContextManagerLink next) {
            this.cm = cm;
            this.next = next;
        }
    }

    private ThreadLocal<ContextManagerLink> threadContextList = new ThreadLocal<>();

    /**
     * Collection of all ContextManagers that are open
     * in the complete Derby system. A ContextManager is
     * added when it is created with newContextManager and
     * removed when the session is closed.
     *
     * @see #newContextManager()
     * @see SystemContext#cleanupOnError(Throwable)
     */
    private final Set<ContextManager> allContexts = new HashSet<>();

    // TODO: remove duplicate
    public static ContextService getService(){
        return getFactory();
    }

    /**
     * Create a new ContextService for a Derby system.
     * Only a single system is active at any time.
     */
    private ContextService(){
        // find the error stream
        errorStream=Monitor.getStream();
    }

    /**
     * So it can be given to us and taken away...
     */
    public static void stop() {
        factory = null;
    }

    public static ContextService getFactory() {
        if (factory == null) {
            synchronized(ContextService.class){
                if (factory == null) {
                    factory = new ContextService();
                }
            }
        }
        return factory;
    }

    /**
     * Find the context with the given name in the context service factory
     * loaded for the system.
     *
     * @return The requested context, null if it doesn't exist.
     */
    public static Context getContext(String contextId) {

        if (factory == null) {
            // The context service is already stopped.
            return null;
        }

        ContextManager cm = factory.getCurrentContextManager();
        if (cm == null) {
            return null;
        }
        return cm.getContext(contextId);
    }

    /**
     * Find the context with the given name in the context service factory
     * loaded for the system.
     * <p/>
     * This version will not do any debug checking, but return null
     * quietly if it runs into any problems.
     *
     * @return The requested context, null if it doesn't exist.
     */
    public static Context getContextOrNull(String contextId){
        return getContext(contextId);
    }


    /**
     * Get current Context Manager linked to the current Thread.
     * See setCurrentContextManager for details.
     * Note that this call can be expensive and is only
     * intended to be used in "stateless" situations.
     * Ideally code has a reference to the correct
     * ContextManager from another Object, such as a pushed Context.
     *
     * @return ContextManager current Context Manager
     */
    public ContextManager getCurrentContextManager(){

        if (factory == null) {
            // The context service is already stopped.
            return null;
        }

        ContextManagerLink link = threadContextList.get();
        if (link == null || link.cm == null) {
            return null;
        }
        ContextManager cm = link.cm;
        if (link.next == null && cm.activeThread != Thread.currentThread()) {
            return null;
        }
        return cm;
    }

    /**
     * Break the link between the current Thread and the passed
     * in ContextManager. Called in a pair with setCurrentContextManager,
     * see that method for details.
     */
    public void resetCurrentContextManager(ContextManager cm){

        if (factory == null) {
            // The context service is already stopped.
            return;
        }

        if(cm.activeCount!=-1){
            if(--cm.activeCount<=0){
                cm.activeThread=null;

                // If the ContextManager is empty
                // then don't keep a reference to it
                // when it is not in use. The ContextManager
                // has been closed (most likely) and this
                // is now unwanted. Keeping the reference
                // would hold onto memory and increase the
                // chance of holding onto a another reference
                // will could cause issues for future operations.
                if(cm.isEmpty()) {
                    threadContextList.set(null);
                }
            }
            return;
        }

        if (LOG.isTraceEnabled()) {
            Thread currThread = Thread.currentThread();
            Thread activeThread = cm.activeThread;
            if (currThread != activeThread) {
                LOG.trace("resetCurrentContextManager - mismatch threads - current"+currThread+" - cm's "+activeThread);
            }
            if (getCurrentContextManager() != cm) {
                LOG.trace("resetCurrentContextManager - mismatch contexts - "+currThread+" : "+getCurrentContextManager()+" : "+cm);
            }
        }

        ContextManagerLink link = threadContextList.get();
        link = link.next;
        threadContextList.set(link);

        ContextManager nextCM = link.cm;
        boolean seenMultipleCM=false;
        boolean seenCM=false;
        int count = 0;
        for (;link != null; link = link.next, ++count){
            ContextManager stackCM = link.cm;
            if (stackCM != nextCM) {
                seenMultipleCM = true;
            }
            if (stackCM == cm) {
                seenCM = true;
            }
        }

        if(!seenCM){
            cm.activeThread=null;
            cm.activeCount=0;
        }

        if(!seenMultipleCM){
            // all the context managers on the stack
            // are the same so reduce to a simple count.
            nextCM.activeCount = count;
            link = new ContextManagerLink(nextCM, null);
            threadContextList.set(link);
        }
    }

    /**
     * The current thread (passed in a me) is setting associateCM
     * to be its current context manager. Sets the thread local
     * variable threadContextList to reflect associateCM being
     * the current ContextManager.
     *
     * @return True if the nesting level is to be represented in
     * the ContextManager.activeCount field. False if not.
     * @see ContextManager#activeCount
     * @see ContextManager#activeThread
     */
    private boolean addToThreadList(ContextManager associateCM) {

        ContextManagerLink link = threadContextList.get();

        // Not currently using any ContextManager
        if (link == null){
            link = new ContextManagerLink(associateCM, null);
            threadContextList.set(link);
            return true;
        }

        // Already set up to reflect associateCM ContextManager
        if (associateCM == link.cm) {
            return true;
        }

        if (link.next == null) {
            // Could be two situations:
            // 1. Single ContextManager not in use by this thread
            // 2. Single ContextManager in use by this thread (nested call)

            ContextManager threadsCM = link.cm;
            if (threadsCM.activeThread != Thread.currentThread()) {
                // Not nested, just a CM left over
                // from a previous execution.
                link.cm = associateCM;
                return true;
            }
            // Nested, need to create a Stack of ContextManagers,
            // the top of the stack will be the active one.
            // The stack represents the true nesting
            // of ContextManagers, splitting out nesting
            // of a single ContextManager into multiple
            // entries in the stack.
            link = null;
            for (int i = 0; i < threadsCM.activeCount; i++) {
                link = new ContextManagerLink(threadsCM, link);
            }
            threadsCM.activeCount = -1;
        }

        link = new ContextManagerLink(associateCM, link);
        threadContextList.set(link);
        associateCM.activeCount=-1;

        if (LOG.isTraceEnabled()) {
            int size = 0;
            for (; link != null; link = link.next) ++size;
            if (size > 10) {
                LOG.trace("memoryLeakTrace:threadLocal " + size);
            }
        }

        return false;
    }

    /**
     * Link the current thread to the passed in Contextmanager
     * so that a subsequent call to getCurrentContextManager by
     * the current Thread will return cm.
     * ContextManagers are tied to a Thread while the thread
     * is executing Derby code. For example on most JDBC method
     * calls the ContextManager backing the Connection object
     * is tied to the current Thread at the start of the method
     * and reset at the end of the method. Once the Thread
     * has completed its Derby work the method resetCurrentContextManager
     * must be called with the same ContextManager to break the link.
     * Note that a subsquent use of the ContextManager may be on
     * a separate Thread, the Thread is only linked to the ContextManager
     * between the setCurrentContextManager and resetCurrentContextManager calls.
     * <BR>
     * ContextService supports nesting of calls by a single Thread, either
     * with the same ContextManager or a different ContextManager.
     * <UL>
     * <LI>The same ContextManager would be pushed during a nested JDBC call in
     * a procedure or function.
     * <LI>A different ContextManager would be pushed during a call on
     * a different embedded JDBC Connection in a procedure or function.
     * </UL>
     */
    public void setCurrentContextManager(ContextManager cm){

        if (factory == null) {
            // The context service is already stopped.
            return;
        }

        if (LOG.isTraceEnabled()) {
            Thread me = Thread.currentThread();
            if (cm.activeThread != null && me != cm.activeThread) {
                LOG.trace("setCurrentContextManager - mismatch threads - current " + me + " - cm's " + cm.activeThread);
            }
        }

        if (cm.activeThread == null) {
            cm.activeThread = Thread.currentThread();
        }
        if (addToThreadList(cm)) {
            cm.activeCount++;
        }
    }

    /**
     * It's up to the caller to track this context manager and set it
     * in the context manager list using setCurrentContextManager.
     * We don't keep track of it due to this call being made.
     */
    public ContextManager newContextManager(){
        ContextManager cm=new ContextManager(this,errorStream);

        // push a context that will shut down the system on
        // a severe error.
        new SystemContext(cm);

        synchronized(allContexts) {
            allContexts.add(cm);
        }

        if (LOG.isTraceEnabled()) {
            if (allContexts.size() > 1000) {
                LOG.trace("memoryLeakTrace:allContexts " + allContexts.size());
            }
        }

        return cm;
    }

    public void notifyAllActiveThreads(Context c){
        Thread me=Thread.currentThread();

        synchronized (allContexts) {
            for(ContextManager cm : allContexts){

                final Thread active = cm.activeThread;
                if (active == null || active == me) {
                    continue;
                }

                if (cm.setInterrupted(c)) {
                    AccessController.doPrivileged((PrivilegedAction) () -> {
                        active.interrupt();
                        return null;
                    });
                }
            }
        }
    }

    /**
     * Remove a ContextManager from the list of all active
     * contexts managers.
     */
    public void removeContext(ContextManager cm){
        synchronized (allContexts) {
            allContexts.remove(cm);
        }
    }

    /**
     * Returns all contexts of a given type (id).
     *
     * @param contextId ConnectionContext.CONTEXT_ID, AccessFactoryGlobals.RAMXACT_CONTEXT_ID,
     *                  AccessFactoryGlobals.USER_TRANS_NAME, LanguageConnectionContext.CONTEXT_ID, etc
     */
    public <T extends Context> List<T> getAllContexts(String contextId){
        List<T> contexts= new ArrayList<>();
        synchronized (allContexts) {
            for (ContextManager contextManager : allContexts) {
                contexts.addAll((Collection<? extends T>) contextManager.getContextStack(contextId));
            }
        }
        return contexts;
    }
}
