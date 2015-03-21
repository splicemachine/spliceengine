package com.splicemachine.hbase.backup;

import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Created by jyuan on 3/20/15.
 */
public class IncrementalBackupIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = IncrementalBackupIT.class.getSimpleName().toUpperCase();
    protected static String TABLE = "A";
}
