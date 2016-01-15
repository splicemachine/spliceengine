package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
public interface DDLWatchChecker{

    boolean initialize(CommunicationListener listener) throws IOException;

    Collection<String> getCurrentChangeIds() throws IOException;

    DDLChange getChange(String changeId) throws IOException;

    void notifyProcessed(Collection<Pair<DDLChange,String>> processedChanges) throws IOException;

    void killDDLTransaction(String key);
}
