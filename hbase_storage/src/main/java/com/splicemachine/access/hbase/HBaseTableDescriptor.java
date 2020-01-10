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

import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Created by jyuan on 3/3/16.
 */
public class HBaseTableDescriptor implements TableDescriptor{

    HTableDescriptor hTableDescriptor;

    public HBaseTableDescriptor(HTableDescriptor hTableDescriptor) {
        this.hTableDescriptor = hTableDescriptor;
    }

    @Override
    public String getTableName() {
        return hTableDescriptor.getNameAsString();
    }

    @Override
    public String getTransactionId() {
        return hTableDescriptor.getValue(SIConstants.TRANSACTION_ID_ATTR);
    }

    @Override
    public String getDroppedTransactionId() {
        return hTableDescriptor.getValue(SIConstants.DROPPED_TRANSACTION_ID_ATTR);
    }

    public HTableDescriptor getHTableDescriptor() {
        return hTableDescriptor;
    }
}
