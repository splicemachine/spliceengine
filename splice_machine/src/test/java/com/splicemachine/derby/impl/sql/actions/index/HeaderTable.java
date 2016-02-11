package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

/**
 * @author P Trolard
 *         Date: 09/09/2013
 */
public class HeaderTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "HEADER";
    public static final String INDEX_NAME = "IDX_HEADER";
    public static final String INDEX_DEF = "(transaction_dt, customer_master_id)";

    private static final String CREATE_STRING = "(" +
            "TRANSACTION_HEADER_KEY BIGINT NOT NULL, " +
            "CUSTOMER_MASTER_ID BIGINT, " +
            "TRANSACTION_DT DATE NOT NULL " +
            ")";

    public HeaderTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }

}
