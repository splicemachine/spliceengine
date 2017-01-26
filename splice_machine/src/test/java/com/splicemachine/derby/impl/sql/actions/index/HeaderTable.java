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
