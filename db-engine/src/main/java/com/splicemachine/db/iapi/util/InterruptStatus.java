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

package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.ShutdownException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

/**
 * Static methods to save and retrieve information about a (session) thread's
 * interrupt status flag. If during operation we notice an interrupt, Derby will
 * either:
 * <ul>
 *    <li>immediately throw an exception to cut execution short, also
 *        resurrecting the thread's interrupted status flag. This does not
 *        require use of this class.
 *
 *    <li>just note the fact using this class ({@code noteAndClearInterrupt},
 *        or ({@code setInterrupted})), and retry whatever got interrupted,
 *        continuing execution. To achieve this, Derby will always temporarily
 *        clear the interrupted status flag.
 *        Later, depending on the type of SQL statement, we may wish to
 *        interrupt execution by throwing an SQLException at a safe place, say,
 *        after a statement in a batch is complete ({@code throwIf}), or just
 *        let the execution run to completion, and then just prior to returning
 *        to the appliction, the thread's interrupted status flag will
 *        resurrected ({@code restoreIntrFlagIfSeen})
 *
 * </ul>
 * Normally, the information is saved away in the session's
 * LanguageConnectionContext, if available. If not, we save it in a thread
 * local variable.
 */

public class InterruptStatus {

    /**
     * Constants used by code that retries file operations after seeing the
     * NIO file channel closed due to interrupts.
     */
    public final static int MAX_INTERRUPT_RETRIES = 120;
    public final static int INTERRUPT_RETRY_SLEEP = 500; // millis

    /**
     * Use thread local variable to store interrupt status flag *only* if we
     * don't have lcc, e.g. during database creation, shutdown etc.
     */
    private static final ThreadLocal exception = new ThreadLocal ();

    /**
     * Make a note that this thread saw an interrupt. Thread's intr
     * status flag is presumably off already, but we reset it here
     * also. Use lcc if available, else thread local variable.
     */
    public static void setInterrupted() {
        LanguageConnectionContext lcc = null;
        try {
            lcc = (LanguageConnectionContext)ContextService.getContextOrNull(
                LanguageConnectionContext.CONTEXT_ID);

        } catch (ShutdownException e) {
            // Ignore. Can happen when: a) background thread (RawStoreDaemon)
            // is performing checkpointing and b) a user thread starts shutdown
            // and interrupts the background thread. During recovery of the
            // container we get here. DERBY-4920.
        }


        Thread.interrupted();

        StandardException e =
            StandardException.newException(SQLState.CONN_INTERRUPT);

        if (lcc != null) {
            lcc.setInterruptedException(e);

        } else {
            exception.set(e);
        }
    }

    /**
     * Use when lcc is dying to save info in thread local instead. Useful under
     * shutdown.
     */
    public static void saveInfoFromLcc(LanguageConnectionContext lcc) {
        
        StandardException e = lcc.getInterruptedException();

        if (e != null) {
            exception.set(e);
        }
    }


    /**
     * Checks if the thread has been interrupted in NIO, presumably because we
     * saw an exception indicating this. Make a note of this and clear the
     * thread's interrupt status flag (NIO doesn't clear it when throwing) so
     * we can retry whatever we are doing. It will be set back ON before
     * control is transferred back to the application, cf. {@code
     * restoreIntrFlagIfSeen}.
     * <p/>
     * The note that we saw an interrupt is stored in the lcc if available, if
     * not, in thread local {@code exception}.
     *
     * @param s (debug info) whence
     * @param threadsInPageIO (debug info) number of threads inside the NIO
     *        code concurrently
     * @param hashCode (debug info) container id
     *
     * @return true if the thread's interrupt status flag was set
     */
    public static boolean noteAndClearInterrupt(String s,
                                                int threadsInPageIO,
                                                int hashCode) {
        if (Thread.currentThread().isInterrupted()) {

            setInterrupted();

            if (SanityManager.DEBUG) {

                if (SanityManager.DEBUG_ON("DebugInterruptRecovery")) {
                    SanityManager.DEBUG_PRINT(
                        "DebugInterruptRecovery",
                        Thread.currentThread().getName() + " " +
                        Integer.toHexString(hashCode) +
                        "@Interrupted: " + s + " threadsInPageIO: " +
                        threadsInPageIO + "\n");
                }
            }

            Thread.interrupted(); // clear status flag

            return true;
        } else {
            return false;
        }
    }


    /**
     * Check if the we ever noticed and reset the thread's interrupt status
     * flag to allow safe operation during execution.  Called from JDBC API
     * methods before returning control to user application. Typically, this
     * happens just prior to return in methods that catch {@code Throwable} and
     * invoke
     * {@code handleException} (directly or indirectly) on it, e.g.
     * <pre>
     *       :
     *       InterruptStatus.restoreIntrFlagIfSeen();
     *       return ...;
     *    } catch (Throwable t) {
     *       throw handleException(t);
     *    }
     * </pre>
     * {@code handleException} does its own calls to {@code
     * restoreIntrFlagIfSeen}. If {@code setupContextStack} has been called
     * consider using the overloaded variant of {@code restoreIntrFlagIfSeen}
     * with an lcc argument.
     * <p/>
     * If an interrupt status flag was seen, we set it back <em>on</em> here.
     */
    public static void restoreIntrFlagIfSeen() {

        LanguageConnectionContext lcc = null;
        try {
            lcc =
                (LanguageConnectionContext)ContextService.getContextOrNull(
                    LanguageConnectionContext.CONTEXT_ID);
        } catch (ShutdownException e) {
            // Ignore. DERBY-4911 Restoring interrupt flag is moot anyway if we
            // are closing down.
        }

        if (lcc == null) {
            // no lcc available for this thread, use thread local flag
            if (exception.get() != null) {

                exception.set(null);

                // Set thread's interrupt status flag back on before returning
                // control to user application
                Thread.currentThread().interrupt();
            }

        } else if (lcc.getInterruptedException() != null) {

            lcc.setInterruptedException(null);

            // Set thread's interrupt status flag back on before returning
            // control to user application
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Same purpose as {@code restoreIntrFlagIfSeen()}. This variant presumes
     * we are sure we have a {@code lcc != null}, i.e. {@code
     * setupContextStack} has been called and not yet restored.  Note that we
     * cannot merge this code with {@code restoreContextStack}, since that is
     * typically called in a {@code finally} block, at which point in time, the
     * {@code lcc} may be gone due to errors of severity {@code
     * SESSION_SEVERITY} or {@code DATABASE_SEVERITY}.
     * <p/>
     * If no {@code lcc} is available, use the zero-arg variant. We only need
     * this variant for performance reasons.
     *
     * @param lcc the language connection context for this session
     */
    public static void restoreIntrFlagIfSeen(LanguageConnectionContext lcc) {

//		if (SanityManager.DEBUG) {
//			LanguageConnectionContext ctxLcc = null;
//			try {
//				ctxLcc = (LanguageConnectionContext)ContextService.
//						getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
//
//				SanityManager.ASSERT(
//						lcc == ctxLcc,
//						"lcc=" + lcc + " getContextOrNull=" + ctxLcc);
//
//			} catch (ShutdownException e) {
//				// ignore
//			}
//		}

        if (lcc.getInterruptedException() != null) {

            lcc.setInterruptedException(null);
            // Set thread's interrupt status flag back on.
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Check if the we ever noticed and reset the thread's interrupt status
     * flag to allow safe operation during execution, or if the interrupt
     * status flag is set now.  Called when operations want to be prematurely
     * terminated due to interrupt.
     * <p/>
     * If an interrupt status flag was seen, but temporarily switched off, we
     * set it back ON here.
     *
     * @param lcc the language connection context for this session
     * @throws StandardException (session level SQLState.CONN_INTERRUPT) if
     *                           interrupt seen
     */
    public static void throwIf(LanguageConnectionContext lcc)
            throws StandardException {

        if (Thread.currentThread().isInterrupted()) {
            setInterrupted();
        }

        StandardException e = lcc.getInterruptedException();

        if (e != null) {
            lcc.setInterruptedException(null);
            // Set thread's interrupt status flag back on:
            // see TransactionResourceImpl#wrapInSQLException

            throw e;
        }

    }
}
