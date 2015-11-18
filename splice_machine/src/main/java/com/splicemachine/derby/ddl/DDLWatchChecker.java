package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;

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

    DDLMessage.DDLChange getChange(String changeId) throws IOException;

    void notifyProcessed(Collection<DDLMessage.DDLChange> processedChanges) throws IOException;

    void killDDLTransaction(String key);
}
