/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
