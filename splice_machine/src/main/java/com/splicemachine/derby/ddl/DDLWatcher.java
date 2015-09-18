package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ddl.DDLChange;

import java.util.Collection;
import java.util.Set;

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
        void startChange(DDLChange change) throws StandardException;

        /**
         * indicates that the specified change completed successfully, and the listener can behave appropriately (
         * e.g. by re-enabling caches, removing from lists, etc.).
         *
         * @param changeId the identifier for the successful change
         */
        void changeSuccessful(String changeId);

        /**
         * indicates that the specified change did <em>not</em> complete successfully. This allows the listener
         * to behave appropariately in scenarios where the change is known to have failed (i.e. in the event of
         * an unexpected failure of the coordinator).
         *
         * @param changeId the identifier for the failed change
         */
        void changeFailed(String changeId);
    }


    void start() throws StandardException;

    Collection<DDLChange> getTentativeDDLs();

    void registerDDLListener(DDLListener listener);

    void unregisterDDLListener(DDLListener listener);

}
