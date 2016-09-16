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

//    @Test
    public void makeOrderUnique() throws Exception{
        String dirName = CsvUtil.getResourceDirectory() + "/index/";
        String sourceFile = "order.csv";
        String targetFile = "order-unique.csv";
        int[] pk = new int[] {0,1,2};
        CsvUtil.writeLines(dirName, targetFile, CsvUtil.makeUnique(dirName,sourceFile,pk));
    }

//    @Test
    public void giveOrderSomeNulls() throws Exception {
        String dirName = CsvUtil.getResourceDirectory() + "/index/";
        String sourceFile = "order.csv";
        String targetFile = "order-with-nulls.csv";
        CsvUtil.writeLines(dirName, targetFile, CsvUtil.insertString(dirName, sourceFile, 5, ""));
    }

}
