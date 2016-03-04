package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface DistributedDataSetProcessor extends DataSetProcessor{

    void setup(Activation activation,String description, String schedulerPool) throws StandardException;

    /**
     * @return if the current thread is in the distributed execution engine or not.
     */
    boolean allowsExecution();
}
