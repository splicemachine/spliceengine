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
}
