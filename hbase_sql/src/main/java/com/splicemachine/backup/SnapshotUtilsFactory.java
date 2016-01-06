package com.splicemachine.backup;


/**
 * Provides factory class for all snapshot utils implementations.
 *
 */
public class SnapshotUtilsFactory {
    public static String SNAPSHOT_UTILS_IMPL_CLASS = 
    		"com.splicemachine.backup.SnapshotUtilsImpl";
    public static SnapshotUtils snapshotUtils;

    static {
        try {
        	snapshotUtils = (SnapshotUtils) Class.forName(SNAPSHOT_UTILS_IMPL_CLASS).newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
