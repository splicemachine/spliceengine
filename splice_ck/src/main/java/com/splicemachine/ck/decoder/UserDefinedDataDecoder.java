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

package com.splicemachine.ck.decoder;

import com.splicemachine.ck.Utils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.*;
import com.splicemachine.utils.Pair;

public class UserDefinedDataDecoder extends UserDataDecoder {

    private final Utils.SQLType[] schema;
    private final int version;

    public UserDefinedDataDecoder(Utils.SQLType[] schema, int version) {
        this.schema = schema;
        this.version = version;
    }

    @Override
    public Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() throws StandardException {
        return Utils.constructExecRowDescriptorSerializer(schema, version, null);
    }
}
