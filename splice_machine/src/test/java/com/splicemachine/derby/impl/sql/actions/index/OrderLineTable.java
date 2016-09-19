/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
