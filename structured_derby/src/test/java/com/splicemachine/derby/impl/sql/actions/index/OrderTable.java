package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

/**
 * @author Jeff Cunningham
 *         Date: 7/31/13
 */
public class OrderTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "OORDER";
    public static final String INDEX_NAME = "IDX_OORDER";
    public static final String INDEX_DEF = "(o_d_id,o_id,o_c_id,o_carrier_id)";
    public static final String INDEX_ORDER_DEF = "(o_carrier_id,o_id,o_d_id,o_c_id)";
    public static final String INDEX_ORDER_DEF_ASC = "(o_carrier_id,o_id ASC,o_d_id,o_c_id)";
    public static final String INDEX_ORDER_DEF_DESC = "(o_carrier_id,o_id DESC,o_d_id,o_c_id)";

    private static String PK = "PRIMARY KEY (o_w_id,o_d_id,o_id)";

    private static final String CREATE_STRING = "(" +
            "  o_w_id int NOT NULL," +
            "  o_d_id int NOT NULL," +
            "  o_id int NOT NULL," +
            "  o_c_id int NOT NULL," +
            "  o_carrier_id int," +
            "  o_ol_cnt decimal(2,0) NOT NULL," +
            "  o_all_local decimal(1,0) NOT NULL," +
            "  o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            PK + ")";

    public OrderTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }

}
