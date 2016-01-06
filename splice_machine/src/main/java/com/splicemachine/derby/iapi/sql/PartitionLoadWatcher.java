package com.splicemachine.derby.iapi.sql;

import com.splicemachine.storage.PartitionLoad;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface PartitionLoadWatcher{

    void startWatching();

    void stopWatching();

    Collection<PartitionLoad> tableLoad(String tableName);
}
