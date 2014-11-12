package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

/**
 * @author Jeff Cunningham
 *         Date: 8/5/13
 */
public class NewOrderTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "NEW_ORDER";
    public static final String INDEX_NAME = "IDX_NEW_ORDER";
    public static final String INDEX_DEF = "(no_w_id,no_o_id)";
    public static final String INDEX_ORDER_DEF = "(no_o_id,no_w_id)";
    public static final String INDEX_ORDER_DEF_ASC = "(no_o_id ASC,no_w_id)";
    public static final String INDEX_ORDER_DEF_DESC = "(no_o_id DESC,no_w_id)";

    private static String PK = "PRIMARY KEY (no_w_id,no_d_id,no_o_id)";

    private static final String CREATE_STRING = "(" +
            " no_w_id int NOT NULL," +
            " no_d_id int NOT NULL," +
            " no_o_id int NOT NULL," +
            PK + ")";

    public NewOrderTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }
}
