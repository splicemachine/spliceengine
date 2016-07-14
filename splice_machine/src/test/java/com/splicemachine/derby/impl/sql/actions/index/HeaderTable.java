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
