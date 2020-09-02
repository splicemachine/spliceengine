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

package com.splicemachine.db.iapi.util;

import com.splicemachine.access.PastTxnIdProvider;
import com.splicemachine.access.PastTxnIdProviderService;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLLongint;

import java.io.IOException;

public class TxnUtil {
    public static NumberDataValue getPastTxn(DataValueDescriptor dvd) throws StandardException {
        if(dvd == null) {
            return new SQLLongint();
        }
        PastTxnIdProvider retriever = PastTxnIdProviderService.loadPropertyManager();
        long result = 0;
        try {
            result = retriever.getPastTxnId(dvd.getTimestamp(null).getTime());
        } catch (IOException e) {
             throw StandardException.plainWrapException(e);
        }
        if(result == -1) {
            return new SQLLongint();
        }
        return new SQLLongint(result);
    }
}
