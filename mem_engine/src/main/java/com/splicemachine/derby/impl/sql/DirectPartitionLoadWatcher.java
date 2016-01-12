package com.splicemachine.derby.impl.sql;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.storage.MPartitionLoad;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.PartitionServer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class DirectPartitionLoadWatcher implements PartitionLoadWatcher{

    @Override
    public void startWatching(){

    }

    @Override
    public void stopWatching(){

    }

    @Override
    public Collection<PartitionLoad> tableLoad(String tableName){
        return Collections.<PartitionLoad>singletonList(new MPartitionLoad(tableName));
    }
}
