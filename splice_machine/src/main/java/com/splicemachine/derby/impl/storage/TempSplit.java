package com.splicemachine.derby.impl.storage;

import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 * Permanently desupported. Do not use.
 */
public class TempSplit {

    // This stored procedure that calls this has been purged.
    // As of version 1.0 (Fuji), it will not automatically be
    // deleted from the data dictionary of an upgraded database.
    // Therefore, to be safe, leave this backing class here
    // but throw runtime exception if invoked.

    private static final Logger LOG = Logger.getLogger(TempSplit.class);

    /**
     * @deprecated No longer supported. No replacement. Throws exception if invoked.
     */
    public static void SYSCS_SPLIT_TEMP() throws SQLException{

        throw new UnsupportedOperationException("This procedure is not supported.");

    }
}
