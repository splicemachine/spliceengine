/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import org.junit.Test;

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
