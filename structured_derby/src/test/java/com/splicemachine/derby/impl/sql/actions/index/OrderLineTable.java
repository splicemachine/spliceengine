package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import org.junit.Test;

/**
 * @author Jeff Cunningham
 *         Date: 7/31/13
 */
public class OrderLineTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "ORDER_LINE";
    public static final String INDEX_NAME = "IDX_ORDER_LINE";
    public static final String INDEX_DEF = "(ol_w_id,ol_d_id,ol_o_id,ol_number)";
    public static final String INDEX_ORDER_DEF = "(ol_o_id,ol_w_id,ol_d_id,ol_number)";
    public static final String INDEX_ORDER_DEF_ASC = "(ol_o_id ASC,ol_w_id,ol_d_id,ol_number)";
    public static final String INDEX_ORDER_DEF_DESC = "(ol_o_id DESC,ol_w_id,ol_d_id,ol_number)";

    private static String PK = "PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)";

    private static final String CREATE_STRING = "(" +
            "  ol_w_id int NOT NULL," +
            "  ol_d_id int NOT NULL," +
            "  ol_o_id int NOT NULL," +
            "  ol_number int NOT NULL," +
            "  ol_i_id int NOT NULL," +
            "  ol_delivery_d timestamp," +
            "  ol_amount decimal(6,2) NOT NULL," +
            "  ol_supply_w_id int NOT NULL," +
            "  ol_quantity decimal(2,0) NOT NULL," +
            "  ol_dist_info varchar(24) NOT NULL," +
            PK + ")";

    public OrderLineTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }

//    @Test
    public void changeOrderLineDecimalFormat() throws Exception {
        String dirName = CsvUtil.getResourceDirectory() + "/index/";
        String sourceFile = "order-line.csv";
        String targetFile = "order-line-decimal.csv";
        CsvUtil.writeLines(dirName, targetFile, CsvUtil.insertString(dirName, sourceFile, 9, "5.0"));
    }

}
