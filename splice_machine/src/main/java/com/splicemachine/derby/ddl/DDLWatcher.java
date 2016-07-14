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
 */

package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;

import java.io.IOException;
import java.util.Collection;

public interface DDLWatcher {

    interface DDLListener{
        /**
         * Indicates that <em>any</em>DDL change has been received, and is being processed. This is useful when
         * managing global behavior based on local events (although not preferable in all cases).
         */
        void startGlobalChange();

        /**
         * Indicates that <em>any</em>DDL change has completed (either successfully or not). This is useful when
         * managing global behavior based on local events (although not preferable in all cases).
         */
        void finishGlobalChange();

        /**
         * Indicates that a change has been received, and work should begin to enter the change into an active
         * state.
         * @param change the change to initiate
         * @throws StandardException if something goes wrong
         */
        void startChange(DDLMessage.DDLChange change) throws StandardException;

        /**
         * indicates that the specified change completed successfully, and the listener can behave appropriately (
         * e.g. by re-enabling caches, removing from lists, etc.).
         *
         * @param change the change for the successful change
         */
        void changeSuccessful(String changeId, DDLMessage.DDLChange change) throws StandardException;

        /**
         * indicates that the specified change did <em>not</em> complete successfully. This allows the listener
         * to behave appropariately in scenarios where the change is known to have failed (i.e. in the event of
         * an unexpected failure of the coordinator).
         *
         * @param changeId the change for the failed change
         */
        void changeFailed(String changeId);
    }


    void start() throws IOException;

    Collection<DDLMessage.DDLChange> getTentativeDDLs();

    void registerDDLListener(DDLListener listener);

    void unregisterDDLListener(DDLListener listener);

    boolean canUseCache(TransactionManager xact_mgr);

    boolean canUseSPSCache(TransactionManager txnMgr);
}
