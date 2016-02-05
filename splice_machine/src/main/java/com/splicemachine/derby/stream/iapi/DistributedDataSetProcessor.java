package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;

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
