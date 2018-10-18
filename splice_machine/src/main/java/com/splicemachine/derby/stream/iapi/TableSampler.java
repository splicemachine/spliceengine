package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by jyuan on 10/9/18.
 */
public interface TableSampler {
    byte[][] sample() throws StandardException;
}
