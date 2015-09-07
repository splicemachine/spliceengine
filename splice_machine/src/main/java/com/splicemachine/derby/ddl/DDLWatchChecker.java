package com.splicemachine.derby.ddl;

import com.splicemachine.pipeline.ddl.DDLChange;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public interface DDLWatchChecker{

    boolean initialize(CommunicationListener listener) throws IOException;

    List<String> getCurrentChangeIds() throws IOException;

    DDLChange getChange(String changeId) throws IOException;

    void notifyProcessed(Collection<DDLChange> processedChanges) throws IOException;

    void killDDLTransaction(String key);
}
