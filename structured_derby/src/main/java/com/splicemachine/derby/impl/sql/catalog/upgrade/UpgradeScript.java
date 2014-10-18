package com.splicemachine.derby.impl.sql.catalog.upgrade;

import org.apache.derby.iapi.error.StandardException;

/**
 * Created by jyuan on 10/17/14.
 */
public interface UpgradeScript {
    void run() throws StandardException;
}
