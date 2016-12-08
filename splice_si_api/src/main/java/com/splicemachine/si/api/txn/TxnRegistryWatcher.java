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

package com.splicemachine.si.api.txn;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Mechanism for "watching" the transaction registry. This is useful for things like keeping the global
 * mat accurate (to within a time frame) via polling or other such mechanism.
 *
 * @author Scott Fines
 *         Date: 11/28/16
 */
public interface TxnRegistryWatcher{

    void start();

    void shutdown();

    /**
     * @param forceUpdate if {@code true}, then the view is forcibly updated, otherwise
     *                    slightly out-of-date information is allowed to be returned.
     * @return a view of the current registry.
     */
    TxnRegistry.TxnRegistryView currentView(boolean forceUpdate) throws IOException;

    /**
     *
     * Register an Action to be taken, when the MAT passes a specific transaction id.
     *
     * For example, suppose you want to perform a certain type of cleanup action (like deleting
     * files from physical storage), but only after you are sure that no one else
     * could possibly be using it(remember in an SI system that there may be other
     * transactions working on an earlier version than you are, and may be looking
     * into the past, so you don't want to physically remove things that they need).
     *
     * To successfully do this, what we really *want* to do is wait until the minimum
     * active transaction has passed ours. Then, once the MAT has passed that point,
     * we can perform whatever action we want because we know that there isn't anyone
     * else in the system who needs anything from us. Of course, this has to wait
     * for the <em>global</em> MAT to pass, or else you ignore distribution challenges.
     *
     * The RegistryAction is an attempt to provide a coherent mechanism for doing this.
     * A RegistryWatcher implementation has a responsibility to call RegistryActions when
     * the MAT has passed the appropriate point.
     *
     * It is important to note that RegistryActions <em>must</em> be thread-safe, as there
     * is no guarantee that the RegistryWatcher will call it from the same thread that it was
     * created on.
     *
     * Once an Action has been taken, it will be discarded and will <em>not</em> be called
     * again. So don't use it as a replacement for repeating tasks.
     *
     * Note that this is a best-effort architecture. Just because an Action is registered does
     * <em>not</em> guarantee that it will ever be called, nor does it guarantee any type of time
     * limit under which it might occur. It simply states that, "To the best of my ability, I'll
     * try and perform this action at some point after the MAT has passes the minTxnId".
     *
     * If the minTxnId has already passed when this is registered (unlikely in most transactional contexts),
     * then the action will be performed immediately.
     *
     * @param minTxnId the transaction id which the MAT must pass before this action can be taken.
     * More formally, this RegistryAction cannot be safely performed unless
     * {@link #currentView(boolean)}.getMinimumActiveTransaction()>requiredMat().
     *
     * @param requiresCommit {@code true} if the txn identified by {@code minTxnId} must be
     * wholly committed in order to perform the action. If {@code false}, then the
     * transaction may have been rolled back, but the action will be performed anyway.
     * @param action the action to perform. Note that any errors thrown by
     *               this are <em>not</em> guaranteed
     *               to propagate <em>anywhere</em>, so try and avoid throwing runtime errors out of this.
     *               Generally implementations will at least log the issue, but you really can't rely on that.
     */
    void registerAction(long minTxnId, boolean requiresCommit,Runnable action);
}
