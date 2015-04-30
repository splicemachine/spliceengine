package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by jleach on 4/17/15.
 */
public interface KeyableRow {
    /**
     *
     * Get an ExecRow representing each keyed value
     */
    public ExecRow getKeyedExecRow (int[] keyColumns) throws StandardException;

    public int hashCode(int[] keysToHash);

    public int compareTo(int[] keysToCompare,ExecRow compareRow);

}