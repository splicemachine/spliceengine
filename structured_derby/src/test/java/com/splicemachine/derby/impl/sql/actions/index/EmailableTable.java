package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

/**
 * @author P Trolard
 *         Date: 09/09/2013
 */


public class EmailableTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "EMAILABLE";

    private static final String CREATE_STRING = "(" +
            "CUSTOMER_MASTER_ID BIGINT" +
            ")";

    public EmailableTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }

}

