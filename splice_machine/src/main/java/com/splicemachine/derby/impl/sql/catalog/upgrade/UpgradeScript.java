package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by jyuan on 10/17/14.
 */
public interface UpgradeScript {
    void run() throws StandardException;
}
