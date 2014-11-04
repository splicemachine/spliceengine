package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;
import com.splicemachine.pipeline.ddl.DDLChange;
import java.util.Set;

public interface DDLWatcher {

    public static interface DDLListener{
        void startGlobalChange();
        void finishGlobalChange();
        void startChange(DDLChange change) throws StandardException;
        void finishChange(String changeId);
    }


    public void start() throws StandardException;

    public Set<DDLChange> getTentativeDDLs();

    public void registerDDLListener(DDLListener listener);

    public void unregisterDDLListener(DDLListener listener);

}
