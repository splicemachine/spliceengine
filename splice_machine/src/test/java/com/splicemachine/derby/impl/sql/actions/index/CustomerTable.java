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
public class CustomerTable extends SpliceTableWatcher {

    public static final String TABLE_NAME = "CUSTOMER";
    public static final String INDEX_NAME = "IDX_CUSTOMER";
    public static final String INDEX_DEF = "(c_w_id,c_d_id,c_last,c_first)";
    public static final String INDEX_ORDER_DEF = "(c_last,c_first,c_w_id,c_d_id)";
    public static final String INDEX_NAME_DEF = "(" +
        "  c_w_id, " +
        "  c_d_id, " +
        "  c_last, " +
        "  c_first, " +
        "  c_middle, " +
        "  c_id, " +
        "  c_street_1, " +
        "  c_street_2, " +
        "  c_city, " +
        "  c_state, " +
        "  c_zip, " +
        "  c_phone, " +
        "  c_credit, " +
        "  c_credit_lim, " +
        "  c_discount,c_balance, " +
        "  c_ytd_payment, " +
        "  c_payment_cnt, " +
        "  c_since)";
    public static final String INDEX_ORDER_DEF_ASC = "(c_last ASC,c_first,c_credit_lim)";
    public static final String INDEX_ORDER_DEF_DESC = "(c_last DESC,c_first,c_credit_lim)";

    private static String PK = "PRIMARY KEY (c_w_id,c_d_id,c_id)";

    private static final String CREATE_STRING = "(" +
            "  c_w_id int NOT NULL," +
            "  c_d_id int NOT NULL," +
            "  c_id int NOT NULL," +
            "  c_discount decimal(4,4) NOT NULL," +
            "  c_credit char(2) NOT NULL," +
            "  c_last varchar(16) NOT NULL," +
            "  c_first varchar(16) NOT NULL," +
            "  c_credit_lim decimal(12,2) NOT NULL," +
            "  c_balance decimal(12,2) NOT NULL," +
            "  c_ytd_payment float NOT NULL," +
            "  c_payment_cnt int NOT NULL," +
            "  c_delivery_cnt int NOT NULL," +
            "  c_street_1 varchar(20) NOT NULL," +
            "  c_street_2 varchar(20) NOT NULL," +
            "  c_city varchar(20) NOT NULL," +
            "  c_state char(2) NOT NULL," +
            "  c_zip char(9) NOT NULL," +
            "  c_phone char(16) NOT NULL," +
            "  c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "  c_middle char(2) NOT NULL," +
            "  c_data varchar(500) NOT NULL," +
            PK + ")";

    public CustomerTable(String tableName, String schemaName) {
        super(tableName,schemaName,CREATE_STRING);
    }


//    @Test
    public void makeCustomerUnique() throws Exception{
        String dirName = CsvUtil.getResourceDirectory() + "/index/";
        String sourceFile = "customer.csv";
        String targetFile = "customer-unique.csv";
        int[] pk = new int[] {0,1,2};
        CsvUtil.writeLines(dirName, targetFile, CsvUtil.makeUnique(dirName,sourceFile,pk));
    }

}
