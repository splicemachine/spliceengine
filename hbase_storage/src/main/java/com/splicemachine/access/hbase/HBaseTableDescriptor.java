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

package com.splicemachine.access.hbase;

import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * Created by jyuan on 3/3/16.
 */
public class HBaseTableDescriptor implements com.splicemachine.access.api.TableDescriptor{

    TableDescriptor tableDescriptor;

    public HBaseTableDescriptor(TableDescriptor tableDescriptor) {
        this.tableDescriptor = tableDescriptor;
    }

    @Override
    public String getTableName() {
        return tableDescriptor.getTableName().getNameAsString();
    }

    @Override
    public String getTransactionId() {
        return tableDescriptor.getValue(SIConstants.TRANSACTION_ID_ATTR);
    }

    @Override
    public String getDroppedTransactionId() {
        return tableDescriptor.getValue(SIConstants.DROPPED_TRANSACTION_ID_ATTR);
    }

    public TableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }
}
